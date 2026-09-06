defmodule CodexPooler.Dev.NativeCompletionDrain do
  @moduledoc false
  use GenServer

  alias CodexPooler.Accounting.Request
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Repo

  @budget 20_000

  @spec hold(Ecto.UUID.t(), Ecto.UUID.t()) :: :ok | {:error, atom()}
  def hold(pool_id, session_id) do
    case GenServer.start(__MODULE__, {pool_id, session_id, self()}, name: __MODULE__) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> {:error, :already_held}
      {:error, _} -> {:error, :active_caller_required}
    end
  end

  @spec command(atom()) :: :ok | {:error, atom()}
  def command(command) do
    case Process.whereis(__MODULE__) do
      nil -> {:error, :capture_required}
      pid -> GenServer.call(pid, command)
    end
  end

  @spec status() :: map()
  def status do
    case Process.whereis(__MODULE__) do
      nil -> %{}
      pid -> GenServer.call(pid, :status)
    end
  end

  @spec cleanup() :: :ok
  def cleanup do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end
  end

  @impl true
  def init({pool_id, session_id, controller}) do
    with {:ok, owner} <- WebsocketOwnerSession.lookup(session_id),
         %{active_turn: %{reply_to: {caller, _}, cleanup_witness: %{request_id: _} = witness}} <-
           :sys.get_state(owner),
         true <- is_pid(caller) and caller != owner,
         %Request{pool_id: ^pool_id, status: "in_progress"} <-
           Repo.get(Request, witness.request_id),
         %CodexTurn{codex_session_id: ^session_id, first_visible_output_at: visible}
         when not is_nil(visible) <- Repo.get_by(CodexTurn, request_id: witness.request_id),
         true <- :erlang.suspend_process(caller) do
      timer = Process.send_after(self(), :release_timeout, @budget)
      Process.monitor(controller)

      {:ok,
       %{
         owner: owner,
         caller: caller,
         held: true,
         request_id: witness.request_id,
         started: false,
         timer: timer,
         timed_out: false
       }}
    else
      _ -> {:stop, :active_caller_required}
    end
  end

  @impl true
  def handle_call(:begin_drain, _from, %{started: false, held: true} = state) do
    :ok = WebsocketOwnerSession.begin_drain(state.owner)
    send(self(), :poll_drain)
    {:reply, :ok, %{state | started: true}}
  end

  def handle_call(:begin_drain, _from, state),
    do: {:reply, {:error, :already_started_or_released}, state}

  def handle_call(:release, _from, state), do: {:reply, :ok, release(state)}

  def handle_call(:status, _from, state) do
    alive = Process.alive?(state.owner)
    terminal = terminal?(state.owner)
    request = Repo.get(Request, state.request_id)
    turn = Repo.get_by(CodexTurn, request_id: state.request_id)
    completed = request.status == "succeeded" and turn.status == CodexTurn.succeeded_status()

    {:reply,
     %{
       caller_held: state.held,
       owner_alive: alive,
       owner_terminal: terminal,
       durable_completed: completed,
       drained: state.started and not alive,
       timed_out: state.timed_out
     }, state}
  end

  @impl true
  def handle_info(:release_timeout, state),
    do: {:noreply, %{release(state) | timed_out: true}}

  def handle_info(:poll_drain, %{timed_out: false} = state) do
    case owner_status(state.owner) do
      {:ok, %{active_turn?: false}} ->
        WebsocketOwnerSession.drain_owner(state.owner)

      {:ok, %{active_turn?: true}} ->
        Process.send_after(self(), :poll_drain, 50)

      _ ->
        :ok
    end

    {:noreply, state}
  end

  def handle_info(:poll_drain, state), do: {:noreply, state}
  def handle_info({:DOWN, _, :process, _, _}, state), do: {:stop, :normal, state}

  @impl true
  def terminate(_reason, state) do
    release(state)
    :ok
  end

  defp release(%{held: true} = state) do
    Process.cancel_timer(state.timer)
    if Process.alive?(state.caller), do: :erlang.resume_process(state.caller)
    %{state | held: false}
  end

  defp release(state), do: state

  defp owner_status(owner) do
    WebsocketOwnerSession.owner_status(owner)
  catch
    :exit, _ -> {:error, :owner_unavailable}
  end

  defp terminal?(owner) do
    is_nil(:sys.get_state(owner).active_turn)
  catch
    :exit, _ -> false
  end
end

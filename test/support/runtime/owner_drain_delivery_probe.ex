defmodule CodexPoolerWeb.Runtime.OwnerDrainDeliveryProbe do
  @moduledoc false

  import ExUnit.Assertions
  import ExUnit.Callbacks

  @budget 15_000

  @spec trace([pid()]) :: :ok
  def trace(producers) do
    Enum.each(producers, fn producer ->
      :erlang.trace(producer, true, [:send, {:tracer, self()}])
    end)

    on_exit(fn ->
      Enum.each(producers, &untrace/1)
    end)

    :ok
  end

  defp untrace(producer) do
    if Process.alive?(producer), do: :erlang.trace(producer, false, [:send])
  end

  @spec assert_no_terminal([pid()]) :: :ok
  def assert_no_terminal(producers) do
    barriers = MapSet.new(producers, &:erlang.trace_delivered/1)
    count = collect(barriers, 0)
    assert count == 0, "terminal notification escaped before cleanup commit release"
    :ok
  end

  defp collect(barriers, count) do
    if MapSet.size(barriers) == 0 do
      count
    else
      receive do
        {:trace_delivered, _producer, reference} ->
          collect(MapSet.delete(barriers, reference), count)

        {:trace, _producer, :send, message, _recipient} ->
          collect(barriers, count + if(terminal?(message), do: 1, else: 0))
      after
        @budget -> flunk("missing owner notification trace delivery barrier")
      end
    end
  end

  defp terminal?({:codex_response_done, _, _}), do: true
  defp terminal?({:websocket_owner_frame, _, _, {:error, _, _}}), do: true
  defp terminal?({:websocket_owner_frame, _, _, _, {:error, _, _}}), do: true
  defp terminal?({_tag, {:websocket_owner_submission_accepted, {:error, _}}}), do: true
  defp terminal?({_tag, {:error, :owner_drained}}), do: true
  defp terminal?(_message), do: false
end

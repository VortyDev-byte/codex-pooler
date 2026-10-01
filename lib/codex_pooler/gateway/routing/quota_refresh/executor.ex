defmodule CodexPooler.Gateway.Routing.QuotaRefresh.Executor do
  @moduledoc """
  Executor for synchronous stale quota refresh plans during routing.
  """

  require Logger

  alias CodexPooler.Gateway.Routing.CandidateEligibility
  alias CodexPooler.Gateway.Routing.QuotaRefresh.Plan
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment

  @quota_refresh_timeout_ms :timer.seconds(3)
  @refresh_backoff_seconds 30
  @total_refresh_timeout_ms :timer.seconds(10)
  @max_refresh_attempts 3

  @spec refresh_stale_candidates(CandidateEligibility.quota_refresh_plan()) ::
          Plan.filter_after_refresh_result()
  def refresh_stale_candidates(refresh_plan) when is_map(refresh_plan) do
    {_attempted, contended?} = refresh_plan
    |> Plan.refresh_candidates()
    |> Enum.reduce_while({0, false}, fn {assignment, _identity}, {attempted, contended?} ->
      result = refresh_assignment_once(assignment)
      if result not in [:already_refreshing, :backoff] do
        Logger.debug(fn ->
          exclusion = Enum.find(refresh_plan.candidate_exclusions,
            &(&1.pool_upstream_assignment_id == assignment.id))
          "quota revalidation assignment_id=#{assignment.id} evidence=#{inspect(exclusion)}"
        end)
      end
      attempted = if result in [:already_refreshing, :backoff], do: attempted, else: attempted + 1
      state = {attempted, contended? or result == :already_refreshing}
      routable? = result == :ok and elem(Plan.filter_after_refresh(refresh_plan), 0) == :ok
      if routable? or attempted >= @max_refresh_attempts, do: {:halt, state}, else: {:cont, state}
    end)

    result = await_refresh_result(refresh_plan, if(contended?, do: 10, else: 0))
    Logger.debug(fn ->
      "quota revalidation result=#{if elem(result, 0) == :ok, do: "eligible", else: "blocked"}"
    end)
    result
  end

  # Contenders share the winner's committed evidence instead of immediately
  # returning a 503 during a reset. Waiting is bounded and never sends traffic.
  defp await_refresh_result(plan, remaining) do
    case Plan.filter_after_refresh(plan) do
      {:error, _} when remaining > 0 and plan.refreshable_candidates != [] ->
        Process.sleep(100)
        await_refresh_result(plan, remaining - 1)

      result -> result
    end
  end

  defp refresh_assignment_once(%PoolUpstreamAssignment{} = assignment) do
    assignment
    |> do_refresh_assignment_once()
    |> log_refresh_result(assignment)
  rescue
    exception in [
      DBConnection.ConnectionError,
      Ecto.Query.CastError,
      Ecto.QueryError,
      Postgrex.Error
    ] ->
      log_refresh_failure(:exception, exception, assignment)

    exception in RuntimeError ->
      log_refresh_failure(:exception, exception, assignment)
  catch
    kind, reason ->
      log_refresh_failure(kind, reason, assignment)
  end

  defp do_refresh_assignment_once(%PoolUpstreamAssignment{} = assignment) do
    token = Ecto.UUID.generate()
    epoch = CredentialFencing.credential_epoch(assignment.upstream_identity_id)

    # No session advisory lock or checked-out connection is held over HTTP:
    # process death must not strand a lock on a connection returned to the pool.
    case Repo.query!("""
         INSERT INTO quota_refresh_leases
           (upstream_identity_id, next_refresh_at, credential_epoch, lease_token, expires_at)
         VALUES ($1, clock_timestamp() + $2 * interval '1 second', $3, $4,
                 clock_timestamp() + $5 * interval '1 millisecond')
         ON CONFLICT (upstream_identity_id) DO UPDATE
         SET next_refresh_at = EXCLUDED.next_refresh_at,
             credential_epoch = EXCLUDED.credential_epoch,
             lease_token = EXCLUDED.lease_token, expires_at = EXCLUDED.expires_at
         WHERE (quota_refresh_leases.next_refresh_at <= clock_timestamp()
           OR quota_refresh_leases.credential_epoch < EXCLUDED.credential_epoch
           OR quota_refresh_leases.next_refresh_at > EXCLUDED.next_refresh_at)
           AND (quota_refresh_leases.expires_at IS NULL
             OR quota_refresh_leases.expires_at <= clock_timestamp()
             OR quota_refresh_leases.expires_at > EXCLUDED.expires_at
             OR quota_refresh_leases.credential_epoch < EXCLUDED.credential_epoch)
         RETURNING upstream_identity_id
         """, [Ecto.UUID.dump!(assignment.upstream_identity_id), @refresh_backoff_seconds,
                  epoch, Ecto.UUID.dump!(token), @total_refresh_timeout_ms]) do
      %{num_rows: 1} ->
        refresh_with_deadline(assignment, token)

      _backoff ->
        case Repo.query!("SELECT expires_at > clock_timestamp() FROM quota_refresh_leases WHERE upstream_identity_id = $1",
               [Ecto.UUID.dump!(assignment.upstream_identity_id)]) do
          %{rows: [[true]]} -> :already_refreshing
          _ -> :backoff
        end
    end
  end

  defp refresh_with_deadline(assignment, token) do
    task = Task.Supervisor.async_nolink(CodexPooler.RateLimitEventSupervisor, fn ->
      Upstreams.reconcile_pool_account(assignment.pool_id, assignment.id,
        receive_timeout: @quota_refresh_timeout_ms)
    end)

    case Task.yield(task, @total_refresh_timeout_ms) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      {:exit, _reason} -> {:error, :quota_refresh_failed}
      nil -> {:error, :quota_refresh_timeout}
    end
  after
    Repo.query!("UPDATE quota_refresh_leases SET expires_at = clock_timestamp() WHERE upstream_identity_id = $1 AND lease_token = $2",
      [Ecto.UUID.dump!(assignment.upstream_identity_id), Ecto.UUID.dump!(token)])
  end

  defp log_refresh_result({:ok, %{quota: %{status: :succeeded}}}, _assignment), do: :ok
  defp log_refresh_result({:ok, _result}, _assignment), do: :error
  defp log_refresh_result(:already_refreshing, _assignment), do: :already_refreshing
  defp log_refresh_result(:backoff, _assignment), do: :backoff

  defp log_refresh_result({:error, reason}, %PoolUpstreamAssignment{} = assignment) do
    log_refresh_failure(:error, reason, assignment)
  end

  defp log_refresh_result(other, %PoolUpstreamAssignment{} = assignment) do
    log_refresh_failure(:unexpected_result, other, assignment)
  end

  defp log_refresh_failure(kind, reason, %PoolUpstreamAssignment{} = assignment) do
    Logger.warning(
      "quota refresh skipped " <>
        "pool_id=#{safe_id(assignment.pool_id)} " <>
        "assignment_id=#{safe_id(assignment.id)} " <>
        "failure_kind=#{failure_kind(kind)} " <>
        "failure_reason=#{failure_reason(reason)}"
    )

    :error
  end

  defp safe_id(value) when is_binary(value), do: value
  defp safe_id(_value), do: "unknown"

  defp failure_kind(kind) when kind in [:error, :exit, :throw], do: Atom.to_string(kind)
  defp failure_kind(kind) when is_atom(kind), do: Atom.to_string(kind)
  defp failure_kind(_kind), do: "unknown"

  defp failure_reason(%module{}) when is_atom(module), do: inspect(module)
  defp failure_reason({reason, _details}) when is_atom(reason), do: Atom.to_string(reason)
  defp failure_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp failure_reason(reason) when is_binary(reason), do: sanitized_reason_token(reason)
  defp failure_reason(_reason), do: "unavailable"

  defp sanitized_reason_token(reason) do
    reason
    |> String.replace(~r/[^a-zA-Z0-9_.:-]+/, "_")
    |> String.slice(0, 80)
    |> case do
      "" -> "binary_reason"
      value -> value
    end
  end
end

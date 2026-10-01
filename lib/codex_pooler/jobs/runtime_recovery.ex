defmodule CodexPooler.Jobs.RuntimeRecovery do
  @moduledoc """
  Recovers derived runtime state independently of the queue it repairs.

  A worker timeout only kills a live executor. After a node dies there is no
  executor to acknowledge its job, and incomplete-job uniqueness would keep
  returning that orphan forever. Rescue only after the worker's timeout plus
  acknowledgement grace, using conditional updates so concurrent rescuers and
  normal job completion cannot overwrite each other.
  """
  use GenServer
  import Ecto.Query
  require Logger

  alias CodexPooler.Catalog
  alias CodexPooler.Gateway.Persistence.RuntimeCleanup
  alias CodexPooler.Jobs.{AccountReconciliationWorker, CatalogSyncWorker}
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Reconciliation.AccountReconciliation

  @interval :timer.minutes(1)
  @grace_seconds 60

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    if Application.get_env(:codex_pooler, :runtime_recovery_enabled, true),
      do: send(self(), :recover)

    {:ok, nil}
  end

  @impl true
  def handle_info(:recover, state) do
    try do
      {:ok, summary} = run()
      if Enum.any?(summary, fn {_key, count} -> count > 0 end),
        do: Logger.info("runtime state recovered #{inspect(summary)}")
    rescue
      exception -> Logger.warning("runtime recovery failed: #{inspect(exception.__struct__)}")
    end

    Process.send_after(self(), :recover, @interval)
    {:noreply, state}
  end

  def run(at \\ DateTime.utc_now()) do
    jobs = recover_worker(CatalogSyncWorker, at) + recover_worker(AccountReconciliationWorker, at)

    with {:ok, catalog} <- Catalog.cleanup_stale_sync_runs(at),
         {:ok, reconciliation} <- AccountReconciliation.cleanup_stale_state(at),
         {:ok, sessions} <- RuntimeCleanup.cleanup_expired_runtime_state(at) do
      {:ok, Map.merge(Map.merge(catalog, reconciliation), sessions) |> Map.put(:recovered_jobs, jobs)}
    end
  end

  def recover_worker(worker, at \\ DateTime.utc_now()) do
    at = DateTime.truncate(at, :microsecond)
    cutoff = DateTime.add(at, -(div(worker.timeout(%Oban.Job{}), 1_000) + @grace_seconds), :second)
    worker_name = Oban.Worker.to_string(worker)

    {count, _} =
      from(job in Oban.Job,
        where: job.worker == ^worker_name and job.state == "executing",
        where: fragment("COALESCE(?, ?) <= ?", job.attempted_at, job.inserted_at, ^cutoff)
      )
      |> Repo.update_all(set: [
        state: dynamic([job], fragment("CASE WHEN ? < ? THEN 'available' ELSE 'discarded' END", job.attempt, job.max_attempts)),
        scheduled_at: at,
        discarded_at: dynamic([job], fragment("CASE WHEN ? >= ? THEN ? ELSE NULL END", job.attempt, job.max_attempts, type(^at, :utc_datetime_usec)))
      ])

    count
  end
end

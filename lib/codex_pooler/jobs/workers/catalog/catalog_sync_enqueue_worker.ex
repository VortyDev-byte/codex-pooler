defmodule CodexPooler.Jobs.CatalogSyncEnqueueWorker do
  @moduledoc """
  Periodically enqueues per-pool catalog sync jobs.
  """

  use Oban.Worker,
    queue: :jobs,
    max_attempts: 3,
    tags: ["catalog_sync_enqueue"],
    unique: [
      fields: [:worker, :queue],
      states: :incomplete,
      period: {5, :minutes}
    ]

  alias CodexPooler.Jobs

  @impl Oban.Worker
  def timeout(%Oban.Job{}), do: :timer.seconds(30)

  @impl Oban.Worker
  def perform(%Oban.Job{}) do
    case Jobs.enqueue_catalog_sync_for_active_pools(trigger_kind: "scheduled") do
      {:ok, %{errors: []}} -> :ok
      {:ok, %{errors: errors}} -> {:error, {:enqueue_failed, errors}}
    end
  end
end

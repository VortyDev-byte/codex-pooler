defmodule CodexPoolerWeb.Runtime.QuotaModelRecoveryTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  alias CodexPooler.{Catalog, FakeUpstream, Repo}
  alias CodexPooler.Catalog.{Model, SyncRun}
  alias CodexPooler.Gateway.Persistence.{BridgeAffinity, BridgeOwnerLease, CodexSession, RoutingCircuitState}
  alias CodexPooler.Jobs.{AccountReconciliationWorker, CatalogSyncWorker, RuntimeRecovery}
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Quota.{AccountAvailabilityStore, RoutingQuotaSnapshot}
  alias CodexPooler.Upstreams.Quota.Windows
  alias CodexPooler.Upstreams.Reconciliation.AccountReconciliation

  setup do
    previous = Application.get_env(:codex_pooler, :preserve_saved_resets)
    Application.put_env(:codex_pooler, :preserve_saved_resets, true)
    on_exit(fn -> Application.put_env(:codex_pooler, :preserve_saved_resets, previous) end)
    :ok
  end

  test "three Plus accounts survive poisoned derived state, a restart and repeated Codex turns" do
    first_server = start_upstream(provider_mode(100))
    second_server = start_upstream(provider_mode(8))
    third_server = start_upstream(provider_mode(12))
    setup = gateway_setup(first_server, quota?: false)
    second = gateway_upstream(setup.pool, second_server, "second-account-token", [])
    third = gateway_upstream(setup.pool, third_server, "third-account-token", [])
    accounts = [%{identity: setup.identity, assignment: setup.assignment}, second, third]
    model = put_model_source_assignments!(setup.model, Enum.map(accounts, & &1.assignment))
    setup = %{setup | model: model}
    at = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    banked = %{"available_count" => 4, "observed_at" => DateTime.to_iso8601(at),
      "credits" => [%{"id" => "banked-reset-sentinel", "expires_at" => DateTime.to_iso8601(DateTime.add(at, 7, :day))}]}

    for account <- accounts do
      account.identity
      |> Ecto.Changeset.change(plan_family: "plus", plan_label: "Plus",
        metadata: Map.put(account.identity.metadata, "saved_resets", banked))
      |> Repo.update!()
    end

    record_window!(setup.identity, 100, at, "codex_usage_api")
    record_window!(second.identity, 100, DateTime.add(at, -60, :second), "codex_rate_limit_event")
    record_window!(second.identity, 8, at, "codex_usage_api", provider_permission: true)
    record_window!(third.identity, 100, DateTime.add(at, -1800, :second), "codex_rate_limit_event", expired: true)
    third.assignment |> Ecto.Changeset.change(health_status: "errored", eligibility_status: "ineligible",
      cooldown_until: DateTime.add(at, 1, :day), metadata: %{"quota_priming" => %{
        "status" => "refreshing", "started_at" => DateTime.to_iso8601(DateTime.add(at, -3600, :second)),
        "credential_epoch" => 1}}) |> Repo.update!()

    Repo.delete_all(Oban.Job)
    catalog_job = stuck_job!(CatalogSyncWorker, %{"pool_id" => setup.pool.id}, at, 1)
    reconciliation_job = stuck_job!(AccountReconciliationWorker, %{"pool_id" => setup.pool.id,
      "pool_upstream_assignment_id" => third.assignment.id}, at, 1)
    run = stale_sync_run!(setup.pool.id, at)
    old_session = stale_session!(setup, at)
    stale_affinity!(setup, at)
    stale_circuit!(setup, second, at)

    # Recovery runs during application boot and periodically without needing a
    # healthy Oban queue. Killing/restarting this supervisor child exercises
    # the boot hook against durable poisoned rows.
    assert :ok = Supervisor.terminate_child(CodexPooler.Supervisor, RuntimeRecovery)
    Application.put_env(:codex_pooler, :runtime_recovery_enabled, true)
    assert {:ok, recovery_pid} = Supervisor.restart_child(CodexPooler.Supervisor, RuntimeRecovery)
    Ecto.Adapters.SQL.Sandbox.allow(Repo, self(), recovery_pid)
    :sys.get_state(recovery_pid)
    Application.put_env(:codex_pooler, :runtime_recovery_enabled, false)
    on_exit(fn ->
      Supervisor.terminate_child(CodexPooler.Supervisor, RuntimeRecovery)
      Supervisor.restart_child(CodexPooler.Supervisor, RuntimeRecovery)
    end)

    assert Repo.reload!(catalog_job).state == "available"
    assert Repo.reload!(reconciliation_job).state == "discarded"
    assert Repo.reload!(run).status == "failed"

    for _ <- 1..8, do: codex_turn!(setup)
    assert response_calls(first_server) == 0
    assert response_calls(second_server) == 8
    assert response_calls(third_server) == 0
    assert Repo.reload!(old_session).status == "closed"

    assert {:ok, %{status: :succeeded} = reconciled} = AccountReconciliation.run(setup.pool.id, third.assignment.id, "scheduled")
    assert reconciled.quota.details["window_count"] > 0
    assert Repo.reload!(third.assignment).health_status == "active"
    assert Repo.reload!(third.assignment).eligibility_status == "eligible"
    assert Repo.reload!(third.assignment).cooldown_until == nil
    snapshot = RoutingQuotaSnapshot.load_by_identity_ids([third.identity.id], DateTime.utc_now())[third.identity.id]
    assert Windows.routing_quota_eligibility_from_snapshot(snapshot).eligible?, inspect(snapshot)

    assert :ok = CatalogSyncWorker.perform(Repo.reload!(catalog_job))
    assert [%Model{status: "active"}] = Catalog.list_models(setup.pool)
    assert Repo.reload!(model).source_assignment_count == 3

    # The previously preferred account now truly runs out. A stale session and
    # cache preference must never stop immediate fallback to the third account.
    record_window!(second.identity, 100, DateTime.utc_now(), "codex_rate_limit_event")
    before = response_calls(third_server)
    for _ <- 1..8, do: codex_turn!(setup)
    assert response_calls(third_server) == before + 8
    assert response_calls(first_server) == 0

    for account <- accounts do
      current = Repo.reload!(account.identity)
      assert current.metadata["saved_resets"] == banked
      assert current.saved_reset_first_seen_ledger == account.identity.saved_reset_first_seen_ledger
      refute Map.has_key?(current.metadata, "saved_reset_redemption")
    end
    refute Repo.exists?(from job in Oban.Job, where: like(job.worker, "%SavedResetRedemptionWorker"))
    refute Enum.any?(Enum.flat_map([first_server, second_server, third_server], &FakeUpstream.requests/1),
      &(&1.method == "POST" and not String.ends_with?(&1.path, "/responses")))
    assert {:ok, %{recovered_jobs: 0}} = RuntimeRecovery.run()
  end

  test "older runtime events and availability cannot overwrite a new credential generation" do
    server = start_upstream(provider_mode(5))
    setup = gateway_setup(server, quota?: false)
    at = DateTime.utc_now()
    record_window!(setup.identity, 100, at, "codex_rate_limit_event")
    old = Repo.reload!(setup.identity)
    current = old |> Ecto.Changeset.change(metadata: CredentialFencing.advance_credential_epoch(old)) |> Repo.update!()
    assert {:ok, []} = Windows.upsert_quota_windows(old, [%{
      quota_key: "account", window_kind: "primary", window_minutes: 300,
      used_percent: 100, reset_at: DateTime.add(at, 1, :hour), source: "codex_rate_limit_event"}])
    record_window!(current, 5, DateTime.add(at, 1, :second), "codex_usage_api", provider_permission: true)
    snapshot = RoutingQuotaSnapshot.load_by_identity_ids([current.id], DateTime.add(at, 2, :second))[current.id]
    assert [window] = RoutingQuotaSnapshot.effective_windows(snapshot)
    assert Decimal.equal?(window.used_percent, 5)
    assert window.metadata["credential_epoch"] == 2

    metadata = %{AccountAvailabilityStore.metadata_key() => AccountAvailabilityStore.encode!(:available, at, 2)}
    blocked = CodexPooler.Quotas.AccountAvailability.new!(:blocked, :blocker, :present)
    assert AccountAvailabilityStore.transition(metadata, blocked, DateTime.add(at, -1, :second), 2) == metadata
    assert AccountAvailabilityStore.transition(metadata, blocked, DateTime.add(at, 1, :second), 1) == metadata
  end

  test "late catalog completion cannot overwrite recovery or a credential replacement" do
    server = start_upstream(provider_mode(5))
    setup = gateway_setup(server)
    assert {:error, :catalog_sync_superseded} = Catalog.sync_pool_catalog(setup.pool, fetcher: fn _source ->
      Catalog.cleanup_stale_sync_runs(DateTime.add(DateTime.utc_now(), 1, :hour))
      {:ok, []}
    end)
    assert Repo.reload!(setup.model).status == "active"

    assert {:error, _run, %{code: :catalog_sync_failed}} = Catalog.sync_pool_catalog(setup.pool, fetcher: fn source ->
      current = Repo.reload!(source.identity)
      current |> Ecto.Changeset.change(metadata: CredentialFencing.advance_credential_epoch(current)) |> Repo.update!()
      {:ok, []}
    end)
    assert Repo.reload!(setup.model).status == "active"
  end

  defp provider_mode(percent) do
    routes = %{
      "/backend-api/wham/usage" => {200, %{"plan_type" => "plus", "saved_resets" => %{"available_count" => 0},
        "rate_limit" => %{"allowed" => percent < 100, "limit_reached" => percent == 100,
          "primary_window" => %{"used_percent" => percent, "limit_window_seconds" => 18000, "reset_after_seconds" => 3600}}}},
      "/backend-api/codex/models" => {200, %{"models" => [%{"id" => "provider-gpt-test-model", "slug" => "gpt-test-model"}]}},
      "/backend-api/codex/responses" => {200, %{"id" => "resp_recovery", "object" => "response",
        "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}}}
    }
    {:path_json, routes |> Map.put("/wham/usage", routes["/backend-api/wham/usage"])
      |> Map.put("/codex/models", routes["/backend-api/codex/models"])}
  end

  defp codex_turn!(setup) do
    conn = Phoenix.ConnTest.build_conn() |> put_req_header("authorization", setup.authorization)
      |> put_req_header("session-id", "poisoned-session")
      |> post("/backend-api/codex/responses", %{"model" => setup.model.exposed_model_id,
        "input" => native_text_input("mock quota recovery turn"), "prompt_cache_key" => "poisoned-cache"})
    assert %{"status" => "completed"} = json_response(conn, 200)
  end

  defp response_calls(server), do: Enum.count(FakeUpstream.requests(server), &String.ends_with?(&1.path, "/responses"))

  defp record_window!(identity, percent, at, source, opts \\ []) do
    metadata = if opts[:provider_permission], do: %{"rate_limit_allowed" => true, "rate_limit_reached" => false}, else: %{}
    assert {:ok, [_]} = Windows.upsert_quota_windows(Repo.reload!(identity), [%{
      quota_key: "account", quota_scope: "account", quota_family: "account", window_kind: "primary",
      window_minutes: 300, used_percent: percent, source: source, source_precision: "authoritative",
      observed_at: at, last_sync_at: at, metadata: metadata,
      reset_at: DateTime.add(at, if(opts[:expired], do: -1, else: 3600), :second)
    }])
  end

  defp stuck_job!(worker, args, at, attempt) do
    {:ok, job} = args |> worker.new(unique: false) |> Oban.insert()
    job |> Ecto.Changeset.change(state: "executing", attempt: attempt,
      attempted_at: DateTime.add(at, -3600, :second)) |> Repo.update!()
  end

  defp stale_sync_run!(pool_id, at) do
    Repo.insert!(%SyncRun{pool_id: pool_id, trigger_kind: "scheduled", status: "running",
      started_at: DateTime.add(at, -3600, :second), discovered_model_count: 0,
      upserted_model_count: 0, stale_marked_count: 0, retired_count: 0, stats: %{}})
  end

  defp stale_session!(setup, at) do
    token = Ecto.UUID.generate()
    past = DateTime.add(at, -60, :second)
    session = Repo.insert!(%CodexSession{pool_id: setup.pool.id, api_key_id: setup.api_key.id,
      session_key: "poisoned-session", pool_upstream_assignment_id: setup.assignment.id, status: "active",
      owner_instance_id: "dead-instance", owner_lease_token: token, owner_lease_expires_at: past,
      last_heartbeat_at: past, created_at: past, updated_at: past})
    Repo.insert!(%BridgeOwnerLease{codex_session_id: session.id, pool_id: setup.pool.id, api_key_id: setup.api_key.id,
      pool_upstream_assignment_id: setup.assignment.id, owner_instance_id: "dead-instance", lease_token: token,
      status: "active", acquired_at: past, renewed_at: past, expires_at: past, metadata: %{}, created_at: past, updated_at: past})
    session
  end

  defp stale_affinity!(setup, at) do
    Repo.insert!(%BridgeAffinity{pool_id: setup.pool.id, api_key_id: setup.api_key.id,
      model_identifier: setup.model.exposed_model_id, affinity_kind: "prompt_cache",
      affinity_key_hash: :crypto.hash(:sha256, "poisoned-cache"), pool_upstream_assignment_id: setup.assignment.id,
      upstream_identity_id: setup.identity.id, status: "active", metadata: %{}, created_at: at, updated_at: at})
  end

  defp stale_circuit!(setup, account, at) do
    past = DateTime.add(at, -3600, :second)
    Repo.insert!(%RoutingCircuitState{pool_id: setup.pool.id, pool_upstream_assignment_id: account.assignment.id,
      upstream_identity_id: account.identity.id, model_identifier: setup.model.exposed_model_id,
      route_class: "http_json", status: "half_open", failure_count: 2, success_count: 0,
      metadata: %{"probe_in_flight_count" => 1}, half_opened_at: past, created_at: past, updated_at: past})
  end
end

defmodule CodexPooler.Catalog.FutureModelsTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Catalog

  test "new account-advertised names and capabilities appear on the next sync" do
    pool = pool_fixture()
    %{assignment: assignment} = active_upstream_assignment_fixture(pool)

    # These are simulated rollout entries, not claims of upstream availability.
    ids = ["gpt-6-sol", "gpt-6-luna", "future-model-#{System.unique_integer([:positive])}"]

    entries =
      Enum.map(ids, fn id ->
        %{
          "slug" => id,
          "display_name" => id,
          "capabilities" => %{"responses" => true, "streaming" => true, "tools" => true},
          "supported_reasoning_levels" => [%{"effort" => "high", "description" => "High"}],
          "context_window" => 123_456,
          "future_capability" => %{"enabled" => true}
        }
      end)

    assert {:ok, _} = Catalog.sync_pool_catalog(pool, fetcher: fn _ -> {:ok, [hd(entries)]} end)
    assert Enum.map(Catalog.list_visible_models(pool), & &1.exposed_model_id) == [hd(ids)]

    assert {:ok, _} = Catalog.sync_pool_catalog(pool, fetcher: fn _ -> {:ok, entries} end)
    models = Catalog.list_visible_models(pool)
    assert Enum.sort(Enum.map(models, & &1.exposed_model_id)) == Enum.sort(ids)

    for model <- models do
      assert model.upstream_model_id == model.exposed_model_id
      assert model.supports_responses
      assert model.supports_streaming
      assert model.supports_tools
      assert model.supports_reasoning
      assert assignment.id in model.metadata["source_assignment_ids"]
      assert model.metadata["upstream_model"]["future_capability"] == %{"enabled" => true}
      assert model.metadata["upstream_model"]["context_window"] == 123_456
    end
  end
end

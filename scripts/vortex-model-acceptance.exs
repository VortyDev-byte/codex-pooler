alias CodexPooler.Catalog
alias CodexPooler.Catalog.Sync.Discovery
alias CodexPooler.Gateway.Payloads.{PayloadNormalizer, RequestOptions}
alias CodexPooler.Upstreams.{CodexClientIdentity, EndpointMetadata, Secrets}

results = for pool <- CodexPooler.Pools.list_active_pools() do
  {:ok, _} = Catalog.sync_pool_catalog(pool)
  models = Catalog.list_visible_models(pool)
  for {source, account} <- Enum.with_index(Catalog.list_catalog_sync_assignments(pool), 1) do
    {:ok, raw_models} = Discovery.fetch_models_for_assignment(source)
    {:ok, token} = Secrets.decrypt_active_secret(source.identity, "access_token")
    {:ok, url} = EndpointMetadata.endpoint_url(source.identity, source.assignment,
      "/backend-api/codex/responses")
    for raw <- raw_models, raw["visibility"] == "list" do
      model = Enum.find(models, &(&1.exposed_model_id == (raw["slug"] || raw["id"])))
      payload = %{"model" => model.exposed_model_id, "instructions" => "Reply exactly: MODEL_OK",
        "input" => [%{"role" => "user", "content" => "Reply exactly: MODEL_OK"}],
        "stream" => true, "store" => false}
      opts = RequestOptions.from_conn_metadata([], "/backend-api/codex/responses", payload)
      {:ok, body} = PayloadNormalizer.upstream_payload(payload, model,
        "/backend-api/codex/responses", opts)
      headers = [{"authorization", "Bearer #{String.trim(token)}"},
        {"chatgpt-account-id", source.identity.chatgpt_account_id},
        {"content-type", "application/json"}, {"accept", "text/event-stream"}] ++ CodexClientIdentity.headers()
      result = case Req.post(url, headers: headers, body: body, retry: false, receive_timeout: 45_000) do
        {:ok, %{status: status, body: response}} when is_binary(response) ->
          events = response |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, "data: "))
            |> Enum.map(&String.replace_prefix(&1, "data: ", ""))
            |> Enum.flat_map(fn line -> case Jason.decode(line) do {:ok, event} -> [event]; _ -> [] end end)
          text = events |> Enum.filter(&(&1["type"] == "response.output_text.delta")) |> Enum.map_join(& &1["delta"])
          %{status: status, completed: Enum.any?(events, &(&1["type"] == "response.completed")), text: text}
        {:ok, %{status: status, body: response}} -> %{status: status, error: response}
        {:error, _} -> %{status: "transport_error"}
      end
      row = %{account: account, display_name: raw["display_name"], raw_id: raw["id"], raw_slug: raw["slug"],
        normalized_id: model.exposed_model_id, routing_id: model.upstream_model_id,
        sent_body: Jason.decode!(body), result: result}
      IO.puts(Jason.encode!(row))
      row
    end
  end
end
File.write!("/tmp/vortex-model-acceptance.json", Jason.encode!(List.flatten(results), pretty: true))

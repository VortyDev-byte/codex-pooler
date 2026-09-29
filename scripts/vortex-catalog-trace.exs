# Run via release RPC. Credentials remain inside the release process.
alias CodexPooler.Catalog
alias CodexPooler.Upstreams.{CodexClientIdentity, EndpointMetadata, Secrets}

for pool <- CodexPooler.Pools.list_active_pools(),
    {source, index} <- Enum.with_index(Catalog.list_catalog_sync_assignments(pool), 1) do
  {:ok, token} = Secrets.decrypt_active_secret(source.identity, "access_token")
  for version <- Enum.uniq([CodexClientIdentity.version(), "0.155.0"]) do
    {:ok, url} = EndpointMetadata.endpoint_url(source.identity, source.assignment,
      "/backend-api/codex/models?client_version=#{version}")
    headers = [{"authorization", "Bearer #{String.trim(token)}"},
      {"chatgpt-account-id", source.identity.chatgpt_account_id},
      {"accept", "application/json"}, {"user-agent", "codex_cli_rs/#{version}"},
      {"originator", "codex_cli_rs"}, {"version", version}]
    case Req.get(url, headers: headers, retry: false, receive_timeout: 30_000) do
      {:ok, %{status: status, body: body}} ->
        File.write!("/tmp/vortex-catalog-#{index}-#{version}.json", Jason.encode!(body))
        models = if is_map(body), do: body["models"] || body["data"] || [], else: []
        IO.inspect(%{account: index, version: version, status: status,
          envelope_keys: if(is_map(body), do: Map.keys(body), else: []),
          models: Enum.map(models, &Map.take(&1, ~w(id slug display_name minimal_client_version)))}, limit: :infinity)
    end
  end
end

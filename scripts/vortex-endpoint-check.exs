alias CodexPooler.{Repo, Access.APIKey}
alias CodexPooler.Gateway.{Metadata, Payloads.RequestOptions}
for pool <- CodexPooler.Pools.list_active_pools(),
    key <- Enum.filter(Repo.all(APIKey), &(&1.pool_id == pool.id and &1.status == "active")) do
  auth = %{api_key: key, pool: pool, api_key_id: key.id, pool_id: pool.id, key_prefix: key.key_prefix}
  opts = RequestOptions.from_conn_metadata([], "/v1/models", %{})
  case Metadata.serve_openai_models(auth, opts) do
    {:ok, response} -> IO.inspect(%{endpoint: "/v1/models", status: response.status,
      ids: Enum.map(response.body["data"], & &1["id"])}, limit: :infinity)
    {:error, error} -> IO.inspect(%{endpoint: "/v1/models", error: error})
  end
end

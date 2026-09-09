alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}
alias CodexPooler.Upstreams.{EndpointMetadata, Secrets, CloudflareCookies}
for assignment <- CodexPooler.Repo.all(PoolUpstreamAssignment) do
  identity = CodexPooler.Repo.get!(UpstreamIdentity, assignment.upstream_identity_id)
  {:ok, token} = Secrets.decrypt_active_secret(identity, "access_token")
  base = EndpointMetadata.usage_base_url(identity, assignment) |> EndpointMetadata.normalize_base_url()
  headers = [{"authorization", "Bearer " <> String.trim(token)}, {"chatgpt-account-id", identity.chatgpt_account_id}, {"cache-control", "no-cache, no-store"}, {"pragma", "no-cache"}]
  for path <- ["/backend-api/wham/usage", "/backend-api/codex/usage"] do
    url = base <> path
    case Req.get(url, headers: CloudflareCookies.request_headers(url, headers), retry: false, receive_timeout: 15000) do
      {:ok, response} ->
        body = if is_binary(response.body), do: (case Jason.decode(response.body) do {:ok, value} -> value; _ -> %{} end), else: response.body
        safe = if is_map(body), do: Map.take(body, ["rate_limit", "additional_rate_limits", "plan_type"]), else: %{}
        IO.inspect(%{identity: identity.id, host: URI.parse(base).host, path: path, status: response.status, cache: Map.take(response.headers, ["age", "date", "cf-cache-status", "cache-control"]), quota: safe}, limit: :infinity)
      {:error, error} -> IO.inspect(%{identity: identity.id, path: path, error: inspect(error.__struct__)})
    end
  end
end

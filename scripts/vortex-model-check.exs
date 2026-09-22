# Run inside the Vortex release with `rpc Code.eval_file(...)`.
# Reports catalog/model availability only; never prints upstream credentials.
alias CodexPooler.Catalog
alias CodexPooler.Catalog.Sync.Discovery
alias CodexPooler.Upstreams.{CodexClientIdentity, Secrets}

for pool <- CodexPooler.Pools.list_active_pools() do
  sources = Catalog.list_catalog_sync_assignments(pool)

  for {source, index} <- Enum.with_index(sources, 1) do
    case Discovery.fetch_models_for_assignment(source) do
      {:ok, models} ->
        IO.inspect(Enum.map(models, &Map.take(&1, ~w(id slug display_name))),
          label: "Account #{index} advertised models"
        )

      {:error, _reason} ->
        IO.puts("Account #{index}: catalog fetch failed")
    end
  end

  # One small request for each exact requested model, using an existing account.
  case List.first(sources) do
    %{identity: identity} ->
      {:ok, token} = Secrets.decrypt_active_secret(identity, "access_token")

      for model <- ~w(gpt-6-sol gpt-6-luna) do
        headers =
          [
            {"authorization", "Bearer #{String.trim(token)}"},
            {"chatgpt-account-id", identity.chatgpt_account_id},
            {"accept", "text/event-stream"}
          ] ++ CodexClientIdentity.headers()

        result = Req.post("https://chatgpt.com/backend-api/codex/responses",
          headers: headers,
          json: %{
            "model" => model,
            "instructions" => "Reply briefly.",
            "input" => [%{"role" => "user", "content" => "Reply OK."}],
            "stream" => true,
            "store" => false
          },
          retry: false,
          receive_timeout: 60_000
        )

        case result do
          {:ok, %{status: status, body: body}} when is_map(body) ->
            IO.inspect(%{model: model, status: status, error: body["error"] || body["detail"]})

          {:ok, %{status: status, body: body}} when is_binary(body) ->
            IO.inspect(%{model: model, status: status,
              completed: String.contains?(body, "response.completed"),
              failed: String.contains?(body, "response.failed")})

          {:error, _reason} ->
            IO.inspect(%{model: model, result: :transport_error})
        end
      end

    _ -> IO.puts("No active account available for live model checks")
  end
end

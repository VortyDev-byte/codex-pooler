$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    docker compose -p vortex-codex-pooler -f docker-compose.yml -f docker-compose.vortex.yml build app
    if ($LASTEXITCODE -ne 0) { throw 'Vortex image build failed.' }
    docker compose -p vortex-codex-pooler -f docker-compose.yml -f docker-compose.vortex.yml up -d --no-build
    if ($LASTEXITCODE -ne 0) { throw 'Vortex startup failed.' }
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        docker compose -p vortex-codex-pooler -f docker-compose.yml -f docker-compose.vortex.yml exec -T app /app/bin/codex_pooler rpc 'case CodexPooler.Jobs.enqueue_catalog_sync_for_active_pools() do {:ok, %{errors: []}} -> :ok; _ -> raise ~s(Catalog enqueue failed) end'
        if ($LASTEXITCODE -eq 0) { break }
        Start-Sleep -Seconds 2
    }
    if ($LASTEXITCODE -ne 0) { throw 'Vortex initial catalog refresh could not be queued.' }
} finally {
    Pop-Location
}

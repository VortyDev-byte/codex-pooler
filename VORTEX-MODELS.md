# Vortex automatic model discovery

Use `scripts/start-vortex.ps1` to rebuild and start this custom checkout.
It uses the `vortex-codex-pooler` Docker Compose project, local image
`vortex-codex-pooler:local`, and existing external database volume
`vortex-codex-pooler-postgres`. The dashboard remains on port 4100.
Docker Desktop restarts reuse the resulting custom container.

The app runs both the scheduler and workers. Every five minutes it fetches
the Codex model catalog for active, eligible accounts and persists newly
advertised models and their per-account capability metadata. No model-name
allowlist or code edit is required for compatible future catalog entries.
The launch script also queues an immediate refresh.

Only upstream-advertised models are exposed. On 2026-09-22, all three connected
accounts advertised `gpt-5.6-sol`, `gpt-5.6-luna`, and `gpt-6-astra`.
Direct probes for `gpt-6-sol` and `gpt-6-luna` returned HTTP 400 with
"model is not supported when using Codex with a ChatGPT account".
Those exact names will be discovered if upstream advertises them later.
The regression tests use simulated entries for those names and an arbitrary
future name; they do not assert live model availability.

Account access, API-key restrictions, and future upstream protocol changes
still apply. Discovery cannot grant access or guarantee compatibility with
an API protocol that has not been released. Clients may need to refresh their
model picker or restart after a catalog update.

The existing image-generation Full-mode fix is included in source. Do not
combine this overlay with `docker-compose.restore.yml`: its old release-specific
BEAM mount is unnecessary for the custom build.

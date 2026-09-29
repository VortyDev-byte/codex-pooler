Application.put_env(:codex_pooler, CodexPooler.Upstreams.CodexClientIdentity,
  default_client_version: "0.159.1")
Code.require_file("/workspace/lib/codex_pooler/upstreams/codex_client_identity.ex")
ExUnit.start()
Code.require_file("/workspace/test/codex_pooler/upstreams/codex_client_identity_test.exs")

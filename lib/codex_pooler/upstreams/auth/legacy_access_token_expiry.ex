defmodule CodexPooler.Upstreams.Auth.LegacyAccessTokenExpiry do
  @moduledoc false

  import Ecto.Query

  alias CodexPooler.Events
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Auth.{AccessTokenExpiry, TokenRefreshMetadata}
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}
  alias CodexPooler.Upstreams.Secrets

  @spec repair(UpstreamIdentity.t()) ::
          {:ok, UpstreamIdentity.t()} | {:error, %{code: atom(), message: String.t()}}
  def repair(%UpstreamIdentity{} = identity) do
    if legacy?(identity.metadata) do
      Repo.transaction(fn -> lock_and_repair(identity) end)
    else
      {:ok, identity}
    end
  end

  defp lock_and_repair(identity) do
    case CredentialFencing.lock_credential_replacement(identity) do
      %UpstreamIdentity{} = locked ->
        repair_locked(locked)

      nil ->
        Repo.rollback(%{
          code: :upstream_identity_not_found,
          message: "upstream identity was not found"
        })
    end
  end

  defp repair_locked(identity) do
    with true <- identity.status != "deleted" and legacy?(identity.metadata),
         {:ok, epoch} <- CredentialFencing.validate_current_credential_epoch(identity),
         {:ok, token} <- Secrets.decrypt_active_secret(identity, "access_token") do
      metadata =
        TokenRefreshMetadata.recover_legacy_access_token_expiry(
          identity.metadata,
          AccessTokenExpiry.resolve(%{access_token: token}),
          epoch
        )

      identity
      |> Ecto.Changeset.change(metadata: metadata)
      |> Repo.update!()
      |> tap(&broadcast_repair/1)
    else
      _ineligible -> identity
    end
  end

  defp broadcast_repair(identity) do
    Repo.all(
      from assignment in PoolUpstreamAssignment,
        where: assignment.upstream_identity_id == ^identity.id and assignment.status != "deleted",
        select: assignment.pool_id,
        distinct: true
    )
    |> Enum.each(fn pool_id ->
      Events.broadcast_upstreams_after_commit(
        pool_id,
        "upstream_access_token_expiry_recovered",
        %{
          upstream_identity_id: identity.id
        }
      )
    end)
  end

  defp legacy?(%{"token_refresh" => %{} = refresh}) do
    not Map.has_key?(refresh, "access_token_expiry")
  end

  defp legacy?(_metadata), do: false
end

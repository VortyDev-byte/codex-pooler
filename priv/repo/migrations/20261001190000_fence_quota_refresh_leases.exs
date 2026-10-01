defmodule CodexPooler.Repo.Migrations.FenceQuotaRefreshLeases do
  use Ecto.Migration

  def change do
    alter table(:quota_refresh_leases) do
      add :credential_epoch, :bigint, null: false, default: 1
      add :lease_token, :uuid
      add :expires_at, :utc_datetime_usec
    end
  end
end

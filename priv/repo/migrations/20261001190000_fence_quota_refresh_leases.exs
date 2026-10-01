defmodule CodexPooler.Repo.Migrations.FenceQuotaRefreshLeases do
  use Ecto.Migration

  def change do
    alter table(:quota_refresh_leases) do
      add :credential_epoch, :bigint, null: false, default: 1
      add :lease_token, :uuid
      add :expires_at, :utc_datetime_usec
    end

    execute """
    UPDATE account_quota_windows AS quota_window
    SET metadata = COALESCE(quota_window.metadata, '{}'::jsonb) ||
      jsonb_build_object('credential_epoch', 1)
    WHERE NOT (COALESCE(quota_window.metadata, '{}'::jsonb) ? 'credential_epoch')
    """, "SELECT 1"
  end
end

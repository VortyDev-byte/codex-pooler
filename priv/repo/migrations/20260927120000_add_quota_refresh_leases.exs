defmodule CodexPooler.Repo.Migrations.AddQuotaRefreshLeases do
  use Ecto.Migration

  def change do
    create table(:quota_refresh_leases, primary_key: false) do
      add :upstream_identity_id, references(:upstream_identities, type: :uuid, on_delete: :delete_all), primary_key: true
      add :next_refresh_at, :utc_datetime_usec, null: false
    end
  end
end

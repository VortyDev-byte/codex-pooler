defmodule CodexPooler.Upstreams.Quota.CreditBalanceStoreTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Quotas.Evidence
  alias CodexPooler.Upstreams.Quota.CreditBalanceStore

  @now ~U[2026-09-01 12:00:00Z]

  test "partial omission keeps the original observation age, explicit zero replaces it" do
    metadata =
      CreditBalanceStore.transition(%{}, %{"credits" => %{"balance" => "601.0"}}, @now, 2)

    assert %{balance: 601, observed_at: @now} = CreditBalanceStore.current(metadata, 2, @now)

    for payload <- [
          %{},
          %{"credits" => nil},
          %{"credits" => %{}},
          %{"credits" => %{"balance" => nil}}
        ] do
      later = DateTime.add(@now, 60)
      assert CreditBalanceStore.transition(metadata, payload, later, 2) == metadata
    end

    assert %{balance: 0} =
             metadata
             |> CreditBalanceStore.transition(
               %{"credits" => %{"balance" => 0}},
               DateTime.add(@now, 60),
               2
             )
             |> CreditBalanceStore.current(2, DateTime.add(@now, 60))

    assert CreditBalanceStore.current(
             metadata,
             2,
             DateTime.add(@now, Evidence.freshness_ttl_seconds() + 1)
           ) == nil

    assert CreditBalanceStore.current(metadata, 3, @now) == nil
    assert CreditBalanceStore.current(metadata, 2, DateTime.add(@now, -1)) == nil
  end

  test "older observations cannot overwrite current credit evidence and malformed values cannot invent zero" do
    metadata = CreditBalanceStore.transition(%{}, %{"credits" => %{"balance" => 9}}, @now, 2)

    assert CreditBalanceStore.transition(
             metadata,
             %{"credits" => %{"balance" => 20}},
             DateTime.add(@now, -1),
             2
           ) == metadata

    assert CreditBalanceStore.transition(%{}, %{"credits" => %{"balance" => "invalid"}}, @now, 2) ==
             %{}

    assert CreditBalanceStore.current(%{"quota_credit_balance" => %{"balance" => 0}}, 2, @now) ==
             nil
  end
end

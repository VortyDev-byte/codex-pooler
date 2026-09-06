defmodule CodexPooler.Catalog.ModelSelectorStateTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Catalog.ModelSelectorState

  test "normalizes legacy aliases and excludes malformed and duplicate selections" do
    state =
      ModelSelectorState.build(
        %{
          "allowed_models_mode" => "none",
          "allowed_models" => [" Sample ", "sample", nil, "bad model"],
          "manual_models" => " CUSTOM, custom\nsecond "
        },
        %{status: :empty, reason: nil},
        []
      )

    assert state.mode == :deny_all_models
    assert state.selected_identifiers == ["sample"]
    assert state.manual_identifiers == ["custom", "second"]
    assert [%{status: :unavailable}] = state.selected_unavailable_chips
    assert Enum.all?(state.manual_chips, &(&1.status == :manual_unverified))
  end

  test "returns structured errors for invalid text inputs and deduplicates valid identifiers" do
    for invalid <- [nil, 12, %{}, "sample\0model"] do
      assert {:error, %{code: :invalid_model_identifier}} =
               ModelSelectorState.validate_manual_model_identifier(invalid)
    end

    assert {:ok, ["sample", "other"]} =
             ModelSelectorState.validate_manual_model_identifiers([" Sample ", "sample", "OTHER"])

    assert {:ok, []} = ModelSelectorState.validate_manual_model_identifiers(nil)
  end

  test "failed catalog without a reason provides an actionable fallback" do
    state = ModelSelectorState.build(%{}, %{status: :failed, reason: nil}, [])
    assert state.catalog.message == "Model catalog sync failed"
    assert state.catalog.requires_acknowledgement?
    assert [%{code: :failed, message: "Model catalog sync failed"}] = state.warnings
  end
end

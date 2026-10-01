defmodule CodexPooler.Upstreams.SavedResets.Preservation do
  @moduledoc """
  Instance-level guard keeping banked reset state outside automatic recovery.
  """

  def enabled?, do: Application.get_env(:codex_pooler, :preserve_saved_resets, true)
end

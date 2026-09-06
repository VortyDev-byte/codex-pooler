defmodule CodexPooler.Gateway.Runtime.Streaming.UsageJsonToken do
  @moduledoc false

  @type number_phase ::
          :sign
          | :zero
          | :integer
          | :dot
          | :fraction
          | :exponent
          | :exponent_sign
          | :exponent_digits
  @type t ::
          :idle
          | :string
          | :escape
          | :surrogate_slash
          | :surrogate_u
          | {:unicode, 0..3, non_neg_integer(), boolean()}
          | {:utf8, 1..3, byte(), byte()}
          | {:literal, binary()}
          | {:number, number_phase()}
  @type token ::
          :string
          | :scalar
          | :object_start
          | :object_end
          | :array_start
          | :array_end
          | :colon
          | :comma
  @type result :: {:more, t()} | {:done, token()} | {:again, token()} | :error

  @spec step(t(), byte()) :: result()
  def step(:idle, byte) when byte in [32, 9, 10, 13], do: {:more, :idle}
  def step(:idle, ?"), do: {:more, :string}
  def step(:idle, ?{), do: {:done, :object_start}
  def step(:idle, ?}), do: {:done, :object_end}
  def step(:idle, ?[), do: {:done, :array_start}
  def step(:idle, ?]), do: {:done, :array_end}
  def step(:idle, ?:), do: {:done, :colon}
  def step(:idle, ?,), do: {:done, :comma}
  def step(:idle, ?t), do: {:more, {:literal, "rue"}}
  def step(:idle, ?f), do: {:more, {:literal, "alse"}}
  def step(:idle, ?n), do: {:more, {:literal, "ull"}}
  def step(:idle, ?-), do: {:more, {:number, :sign}}
  def step(:idle, ?0), do: {:more, {:number, :zero}}
  def step(:idle, byte) when byte in ?1..?9, do: {:more, {:number, :integer}}
  def step(:string, ?"), do: {:done, :string}
  def step(:string, ?\\), do: {:more, :escape}
  def step(:string, byte) when byte in 32..127, do: {:more, :string}
  def step(:string, byte) when byte in 0xC2..0xDF, do: {:more, {:utf8, 1, 0x80, 0xBF}}
  def step(:string, 0xE0), do: {:more, {:utf8, 2, 0xA0, 0xBF}}
  def step(:string, 0xED), do: {:more, {:utf8, 2, 0x80, 0x9F}}
  def step(:string, byte) when byte in 0xE1..0xEF, do: {:more, {:utf8, 2, 0x80, 0xBF}}
  def step(:string, 0xF0), do: {:more, {:utf8, 3, 0x90, 0xBF}}
  def step(:string, byte) when byte in 0xF1..0xF3, do: {:more, {:utf8, 3, 0x80, 0xBF}}
  def step(:string, 0xF4), do: {:more, {:utf8, 3, 0x80, 0x8F}}
  def step({:utf8, 1, low, high}, byte) when byte >= low and byte <= high, do: {:more, :string}

  def step({:utf8, left, low, high}, byte) when byte >= low and byte <= high,
    do: {:more, {:utf8, left - 1, 0x80, 0xBF}}

  def step(:escape, byte) when byte in [?", ?\\, ?/, ?b, ?f, ?n, ?r, ?t], do: {:more, :string}
  def step(:escape, ?u), do: {:more, {:unicode, 0, 0, false}}
  def step(:surrogate_slash, ?\\), do: {:more, :surrogate_u}
  def step(:surrogate_u, ?u), do: {:more, {:unicode, 0, 0, true}}

  def step({:unicode, digits, value, low?}, byte) do
    case hex(byte) do
      nil -> :error
      digit -> unicode(digits + 1, value * 16 + digit, low?)
    end
  end

  def step({:literal, <<byte>>}, byte), do: {:done, :scalar}
  def step({:literal, <<byte, rest::binary>>}, byte), do: {:more, {:literal, rest}}
  def step({:number, phase}, byte), do: number(phase, byte)
  def step(_state, _byte), do: :error

  defp hex(byte) when byte in ?0..?9, do: byte - ?0
  defp hex(byte) when byte in ?a..?f, do: byte - ?a + 10
  defp hex(byte) when byte in ?A..?F, do: byte - ?A + 10
  defp hex(_byte), do: nil

  defp unicode(digits, value, low?) when digits < 4, do: {:more, {:unicode, digits, value, low?}}
  defp unicode(4, value, true) when value in 0xDC00..0xDFFF, do: {:more, :string}
  defp unicode(4, _value, true), do: :error
  defp unicode(4, value, false) when value in 0xD800..0xDBFF, do: {:more, :surrogate_slash}
  defp unicode(4, value, false) when value in 0xDC00..0xDFFF, do: :error
  defp unicode(4, _value, false), do: {:more, :string}

  defp number(:sign, ?0), do: {:more, {:number, :zero}}
  defp number(:sign, byte) when byte in ?1..?9, do: {:more, {:number, :integer}}
  defp number(:integer, byte) when byte in ?0..?9, do: {:more, {:number, :integer}}
  defp number(phase, ?.) when phase in [:zero, :integer], do: {:more, {:number, :dot}}

  defp number(phase, byte) when phase in [:zero, :integer, :fraction] and byte in [?e, ?E],
    do: {:more, {:number, :exponent}}

  defp number(phase, byte) when phase in [:dot, :fraction] and byte in ?0..?9,
    do: {:more, {:number, :fraction}}

  defp number(:exponent, byte) when byte in [?+, ?-], do: {:more, {:number, :exponent_sign}}

  defp number(phase, byte)
       when phase in [:exponent, :exponent_sign, :exponent_digits] and byte in ?0..?9,
       do: {:more, {:number, :exponent_digits}}

  defp number(phase, byte)
       when phase in [:zero, :integer, :fraction, :exponent_digits] and
              byte in [32, 9, 10, 13, ?,, ?], ?}],
       do: {:again, :scalar}

  defp number(_phase, _byte), do: :error
end

defmodule CodexPooler.Dev.NativePreAttemptDrain.Plug do
  @moduledoc false
  @behaviour Plug
  import Plug.Conn
  alias CodexPooler.Access
  alias CodexPooler.Dev.NativePreAttemptDrain
  alias CodexPoolerWeb.Plugs.TrustedProxyRemoteIp

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    if conn.remote_ip == {127, 0, 0, 1} &&
         TrustedProxyRemoteIp.immediate_peer_ip(conn) == {127, 0, 0, 1} do
      with [header] <- get_req_header(conn, "authorization"),
           {:ok, auth} <- Access.authenticate_authorization_header(header),
           true <- NativePreAttemptDrain.authorized_pool?(auth.pool_id) do
        dispatch(assign(conn, :drain_pool_id, auth.pool_id))
      else
        _ -> json(conn, 403, %{error: "authorized_pool_required"})
      end
    else
      json(conn, 403, %{error: "loopback_required"})
    end
  end

  defp dispatch(%{method: "GET", path_info: []} = conn),
    do: json(conn, 200, NativePreAttemptDrain.status())

  defp dispatch(%{method: "POST", path_info: ["arm"]} = conn) do
    with {:ok, params} <- body(conn),
         true <- params == %{},
         :ok <- NativePreAttemptDrain.arm(conn.assigns.drain_pool_id) do
      json(conn, 200, NativePreAttemptDrain.status())
    else
      _ -> json(conn, 400, %{error: "invalid_arm"})
    end
  end

  defp dispatch(%{method: "POST", path_info: ["drain"]} = conn) do
    result =
      with {:ok, params} <- body(conn), true <- params == %{}, do: NativePreAttemptDrain.drain()

    case result do
      :ok ->
        json(conn, 200, NativePreAttemptDrain.status())

      {:error, reason} when reason in [:capture_required, :owner_unavailable] ->
        json(conn, 409, %{error: reason})

      _ ->
        json(conn, 400, %{error: "invalid_control"})
    end
  end

  defp dispatch(%{method: "POST", path_info: ["disarm"]} = conn) do
    with {:ok, params} <- body(conn), true <- params == %{} do
      :ok = NativePreAttemptDrain.disarm()
      json(conn, 200, NativePreAttemptDrain.status())
    else
      _ -> json(conn, 400, %{error: "invalid_control"})
    end
  end

  defp dispatch(conn), do: json(conn, 404, %{error: "not_found"})

  defp body(%{body_params: %Plug.Conn.Unfetched{}} = conn) do
    with {:ok, raw, _} <- read_body(conn, length: 1024), do: Jason.decode(raw)
  end

  defp body(%{body_params: params}), do: {:ok, params}

  defp json(conn, status, body),
    do:
      conn |> put_resp_content_type("application/json") |> send_resp(status, Jason.encode!(body))
end

defmodule CodexPoolerWeb.V1.FutureModelsTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 2, start_upstream: 1]

  alias CodexPooler.FakeUpstream

  test "advertised future models are listed and routed without a name allowlist", %{conn: conn} do
    for model <- ["gpt-6-sol", "gpt-6-luna", "future-model-#{System.unique_integer([:positive])}"],
        stream? <- [false, true] do
      upstream =
        start_upstream(
          FakeUpstream.sse_stream(
            [
              {"response.completed",
               %{
                 "type" => "response.completed",
                 "response" => %{
                   "id" => "resp_future_model",
                   "status" => "completed",
                   "output" => [],
                   "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}
                 }
               }}
            ],
            done: false
          )
        )

      setup = gateway_setup(upstream, exposed_model_id: model, upstream_model_id: model)
      listed = conn |> recycle() |> auth(setup) |> get("/v1/models") |> json_response(200)
      assert Enum.any?(listed["data"], &(&1["id"] == model))

      response =
        conn
        |> recycle()
        |> auth(setup)
        |> post("/v1/responses", %{
          "model" => model,
          "input" => "Reply OK.",
          "stream" => stream?,
          "reasoning" => %{"effort" => "high"},
          "tools" => [
            %{
              "type" => "function",
              "name" => "lookup",
              "parameters" => %{
                "type" => "object",
                "properties" => %{}
              }
            }
          ]
        })

      if stream? do
        assert response.status == 200
        assert response.resp_body =~ "response.completed"
      else
        assert json_response(response, 200)["id"] == "resp_future_model"
      end

      assert [captured] = FakeUpstream.requests(upstream)
      assert captured.json["model"] == model
      assert captured.json["reasoning"]["effort"] == "high"
      assert [%{"name" => "lookup"}] = captured.json["tools"]
    end
  end
end

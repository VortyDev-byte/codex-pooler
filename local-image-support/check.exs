source = File.read!("/tmp/vortex-pre-dispatch.ex")
source = source |> String.replace("Mix.env() == :test", "false") |> String.replace("defp image_tool_full_mode", "def image_tool_full_mode")
Code.compiler_options(ignore_module_conflict: true)
Code.compile_string(source)
alias CodexPooler.Gateway.Runtime.Dispatch.PreDispatch
lite = %{configured_mode: "auto", effective_mode: "lite", source: "catalog"}
resolutions = %{"chosen-model" => {:ok, lite}}
model = %{exposed_model_id: "chosen-model"}
^resolutions = PreDispatch.image_tool_full_mode(resolutions, model, %{"tools" => [%{"type" => "function", "name" => "code"}]})
^resolutions = PreDispatch.image_tool_full_mode(resolutions, model, %{})
%{"chosen-model" => {:ok, %{effective_mode: "full"}}} = PreDispatch.image_tool_full_mode(resolutions, model, %{"tools" => [%{"type" => "image_generation"}]})
IO.puts("PASS: code tools unchanged; no-tool requests unchanged; image tools use Full")

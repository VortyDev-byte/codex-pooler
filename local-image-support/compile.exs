Code.compiler_options(ignore_module_conflict: true)
source = File.read!("/tmp/vortex-pre-dispatch.ex") |> String.replace("Mix.env() == :test", "false")
[{module, binary}] = Code.compile_string(source, "/tmp/vortex-pre-dispatch.ex")
true = module == CodexPooler.Gateway.Runtime.Dispatch.PreDispatch
File.write!("/tmp/vortex-image-support.beam", binary)
IO.puts("Image request dispatch module compiled successfully")

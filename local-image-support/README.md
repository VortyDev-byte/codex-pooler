# Vortex image support fix

Only requests declaring image_generation select Full instead of Lite mode. No database, account, pool setting, model name, or text/code request policy was changed. Source change: lib/codex_pooler/gateway/runtime/dispatch/pre_dispatch.ex (21 added lines).

Live test: gpt-5.6-sol returned a valid PNG (2,657,381 bytes). Regression: no-tools and function-tool requests preserve their resolutions; image tools use Full.

The installed 0.7.1 release module was backed up as original.beam. The tested compiled module is mounted by the existing docker-compose.restore.yml so ordinary restarts/recreation preserve the fix. Restore the backed-up compose file and original module to roll back. This compiled module is release-specific; before upgrading Pooler, remove its bind mount and build the source change against the new release. Do not reuse this beam blindly with a different release.

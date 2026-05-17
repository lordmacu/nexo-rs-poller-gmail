# nexo-poller-gmail

> Gmail (Google API) poller plugin for [Nexo](https://github.com/lordmacu/nexo-rs) agents (out-of-tree subprocess).

**v0.1.0 — scaffold.** Manifest + trait skeleton + broker round-trip
verified. Full port of fetch + historyId diff + dispatch from
in-tree `nexo-poller::builtins::gmail` is a Phase 96 follow-up.

Scope expansion of Phase 96 — `gmail` was originally missed in the
spec but is provider-specific and kept `nexo-plugin-google` in the
daemon's dep tree until extracted. With this plugin + the
`nexo-poller-google-calendar` sister plugin, `nexo-plugin-google`
is fully out of the workspace.

## Status

| Component | State |
|-----------|-------|
| `[plugin.poller]` manifest | ✅ |
| `PollerHandler` skeleton | ✅ stub (no-op tick) |
| Reverse-RPC `credentials_get` | ✅ wired |
| OAuth refresh + Gmail API fetch | ⬜ pending |
| historyId cursor encoding | ⬜ pending |
| Tests | ⬜ pending (2 unit-tests scaffolded) |
| crates.io publish | ⬜ pending |
| CI workflow | ⬜ pending |

## License

MIT OR Apache-2.0.

# nexo-poller-gmail

> Gmail (Google API) poller plugin for [Nexo](https://github.com/lordmacu/nexo-rs) agents (out-of-tree subprocess).

**v0.1.0 — full port shipped.** Gmail search → regex extract →
outbound dispatch → optional mark-read. Ported from
`nexo-poller::builtins::gmail` (V1) during Phase 96.

Scope expansion of Phase 96 — `gmail` was originally missed in the
spec but is provider-specific and kept `nexo-plugin-google` in the
daemon's dep tree until extracted. With this plugin + the
`nexo-poller-google-calendar` sister plugin, `nexo-plugin-google`
is fully out of the workspace.

## What it does

- Gmail `users/messages` list with `is:unread` / custom query +
  `newer_than:` time bound. Returns up to `max_per_tick` ids.
- Per-id fetch with `format=full` to read subject / from / snippet
  + multipart body (text/plain preferred, text/html fallback with
  tag stripping).
- Optional `sender_allowlist` substring filter on the `From:`
  header.
- Named regex extract over body / snippet → `{field}` placeholder
  substitution in `message_template`. Empty required fields skip
  dispatch (mark-read still fires).
- Dispatch via `host.broker_publish` to
  `plugin.outbound.<channel>.<account_id>` resolved through the
  outbound `credentials_get` call.
- Belt-and-suspenders dedup cursor (5000-id bounded set) catches
  the rare case where dispatch succeeds but `mark_read` fails.

## Operator YAML

```yaml
# pollers.yaml fragment
jobs:
  - id: ana_leads
    kind: gmail
    agent: ana
    schedule: { every: 2m }
    config:
      query: "is:unread subject:lead"
      newer_than: "1d"
      max_per_tick: 10
      dispatch_delay_ms: 1500
      sender_allowlist: ["crm-noreply@", "leads@"]
      extract:
        amount: "Total: \\$([0-9.]+)"
      require_fields: ["amount"]
      message_template: "{from} — {subject} (${amount})"
      mark_read_on_dispatch: true
      deliver:
        channel: whatsapp
        to: "+573001234567"
```

## Status

| Component | State |
|-----------|-------|
| `[plugin.poller]` manifest | ✅ |
| OAuth refresh via `nexo-plugin-google` | ✅ |
| Gmail API fetch + body extraction | ✅ |
| Regex extract + template render | ✅ |
| Mark-read via `messages/{id}/modify` | ✅ |
| Belt-and-suspenders seen-id cursor | ✅ |
| Tests | ✅ 12/12 (config + render + classify + body extract + header) |
| crates.io publish | ⬜ pending Phase 96 release wave |
| CI workflow | ⬜ pending |
| `historyId` migration (non-breaking) | ⬜ deferred |

## License

MIT OR Apache-2.0.

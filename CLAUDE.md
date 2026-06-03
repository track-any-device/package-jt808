# package-jt808 — AI Instructions

This is the **JT/T 808-2019 protocol package** for the Track Any Device platform.
Packagist: `track-any-device/jt808` | Namespace: `TrackAnyDevice\Jt808\`

This package is the PHP side of the JT808 pipeline. The Go server (`server-jt808`) handles
the raw TCP connection and publishes decoded frames to a Redis Stream. This package consumes
that stream, translates frames into platform signals, and dispatches commands back to the
Go server via Redis pub/sub.

Read this file before making any change.

---

## Platform-Wide Rules

These three rules apply in every repository under the `track-any-device` organisation.

**Cross-repo changes: file a GitHub issue first.**
If a task in this repository requires a change in another package or server app — stop. Open a
GitHub issue in the target repository describing exactly what is needed and why. Reference that
issue number in your commit message (`ref track-any-device/{repo}#{n}`). Do not directly edit
files in another repository. When picking up a cross-repo issue, run Claude locally inside that
repository's working directory and work only within its scope.

**Release order: packages before server apps.**
This package depends on `package-core`. Release order: `package-core → package-jt808 → server apps`.
Changes to the Redis Stream schema must be coordinated with `server-jt808` (Go) via a cross-repo issue.

**Database layer lives in `package-core` only.**
No migrations or model classes here. Device records are managed by `package-core`.

---

## Rule 1 — Plan before implementing

Before writing any code, ask clarifying questions. Present a plan and get explicit agreement.
Only begin once the approach is confirmed.

---

## Pipeline Architecture

```
Device (TCP) → server-jt808 (Go)
  → frame decode → Redis Stream XADD jt808:telemetry {msg_type, phone, payload}
  → php artisan jt808:consume (this package's Artisan command)
  → ConsumeJt808Stream job → AOT120Driver / P901Driver → SignalObject
  → SignalService::record() (package-core)

Outbound commands:
  DeviceCommandService (package-core)
  → Redis PUBLISH jt808:cmd:{phone} {command_payload}
  → server-jt808 reads, encodes JT808 frame, sends over TCP socket
```

---

## Rule 2 — Never write to device tables directly

All signal persistence goes through `SignalService::record()` from `package-core`.
This package only translates frames — it does not write to the database directly.

---

## Rule 3 — Redis Stream key names are a shared contract with `server-jt808`

The stream key (`jt808:telemetry`) and command channel pattern (`jt808:cmd:{phone}`) are
defined in both this package and the Go server. Never rename them here without filing an issue
against `server-jt808` and coordinating the change.

---

## Rule 4 — Unrecognised devices are auto-created as `pending`

When a registration frame (0x0100) arrives for an unknown IMEI, the consumer creates a
`Device` record with `status = DeviceStatus::Pending`. Central staff approve it in Filament.
Do not skip this step or auto-approve devices.

---

## Supported Message Types

| Hex | Name | Handler |
|---|---|---|
| `0x0100` | Registration | Creates/updates device record, responds 0x8100 |
| `0x0102` | Authentication | Validates token, responds 0x8001 |
| `0x0200` | Location report | Parses to SignalObject, calls SignalService |
| `0x0002` | Heartbeat | Updates last_seen, responds 0x8001 |

---

## Dependencies

```
track-any-device/core
```

---

## Versioning

Tags are created automatically on merge to `main`. Default bump is `patch`.

# track-any-device/jt808

JT/T 808-2019 protocol driver, connector, and Redis Stream consumer for the **Track Any Device** platform.

Devices communicate over a persistent TCP connection handled by the companion Go service (`jt808-server`). The Go service decodes binary frames, publishes normalised JSON events onto a Redis Stream (`jt808:telemetry`), and subscribes to a Redis pub/sub channel (`jt808:cmd:{phone}`) for outgoing commands.

---

## Requirements

| Dependency | Version |
|---|---|
| PHP | ^8.3 |
| Laravel | ^13.7 |
| `track-any-device/core` | (sibling package — must be installed) |
| Redis | 6.2+ (XAUTOCLAIM support) |
| Go JT808 server | running and connected to the same Redis instance |

---

## Installation

```bash
composer require track-any-device/jt808
```

Publish the config stub:

```bash
php artisan vendor:publish --tag=jt808-config
```

---

## Environment variables

Add to the host app's `.env`:

```dotenv
JT808_SERVER_HOST=your-tcp-server-host
JT808_SERVER_PORT=7018
JT808_REDIS_CONNECTION=jt808
```

---

## Redis connection

The package exclusively uses the named Redis connection `jt808` (overridable via `JT808_REDIS_CONNECTION`). Add it to `config/database.php`:

```php
'redis' => [
    // ...
    'jt808' => [
        'url'      => env('REDIS_URL'),
        'host'     => env('REDIS_HOST', '127.0.0.1'),
        'password' => env('REDIS_PASSWORD'),
        'port'     => env('REDIS_PORT', '6379'),
        'database' => env('REDIS_JT808_DB', '1'),
    ],
],
```

---

## Config (`config/jt808.php`)

```php
return [
    'server_host'      => env('JT808_SERVER_HOST', ''),
    'server_port'      => (int) env('JT808_SERVER_PORT', 7018),
    'redis_connection' => env('JT808_REDIS_CONNECTION', 'jt808'),
];
```

---

## Host-app contracts

### Device type seed

The stream consumer looks up the device type by slug `'jt808'`. Ensure it exists before starting the consumer:

```bash
php artisan db:seed --class=DeviceTypeSeeder
```

### Driver registration

Register `Jt808Driver` in the core `DeviceServiceProvider` for the `jt808` slug so `DeviceServiceProvider::driverFor('jt808')` resolves correctly.

### Auth / authorization

`addOnCommand()` calls `auth()->id()` to record who requested a command. In CLI/queue contexts this returns `null` — that is acceptable. For Filament or API callers, ensure a guard is active before dispatching add-on commands.

---

## Starting the stream consumer

```bash
php artisan jt808:consume
```

| Option | Default | Description |
|---|---|---|
| `--stream` | `jt808:telemetry` | Redis Stream key published by the Go server |
| `--group` | `laravel` | Consumer group name |
| `--consumer` | hostname | Unique consumer name per replica |
| `--batch` | `50` | Messages read per XREADGROUP call |
| `--block` | `2000` | Block timeout (ms) when stream is empty |

Run as a supervised long-lived process (Supervisor, systemd, or a Kubernetes Deployment). The command traps `SIGTERM`/`SIGINT` for graceful shutdown and re-claims any unacknowledged pending messages on startup.

---

## Event flow

```
Device (TCP)
  └─▶ Go jt808-server
        ├─▶ Redis XADD jt808:telemetry  (inbound telemetry)
        │       └─▶ ConsumeJt808Stream (this package)
        │               └─▶ SignalService → InfluxDB + MySQL snapshot
        │
        └─▶ Redis SUBSCRIBE jt808:cmd:{phone}  (outbound commands)
                └─▶ Go jt808-server → binary JT808 frame → Device
```

---

## Supported stream events

| `event` field | Handler | Description |
|---|---|---|
| `device.registered` | `handleRegistration` | First TCP registration frame (0x0100) |
| `device.authenticated` | `handleAuthenticated` | Auth/login frame; updates `last_seen_at` |
| `location` | `handleLocation` | Location report (0x0200) — persists signal, dispatches alarm jobs |

---

## Add-on commands

Commands are available to Filament users via the core add-on command interface.

### Tracking

| Command | Parameters | JT808 msg\_id |
|---|---|---|
| `query_location` | — | 0x8201 |
| `set_report_interval` | `seconds` (int) | 0x8103 |
| `set_heartbeat_interval` | `seconds` (int) | 0x8103 |

### Network

| Command | Parameters | JT808 msg\_id |
|---|---|---|
| `set_server` | `host` (string), `port` (int) | 0x8103 |
| `set_apn` | `apn` (string) | 0x8103 |

### Utility

| Command | Parameters | JT808 msg\_id |
|---|---|---|
| `restart` | — | 0x8105 |
| `factory_reset` | — | 0x8105 |
| `enable_acc` | — | 0x8103 |

---

## Release workflow

Releases are created automatically on every push to `main` by `.github/workflows/release.yml`. Tags follow semver (`vMAJOR.MINOR.PATCH`). The bump type is inferred from [Conventional Commits](https://www.conventionalcommits.org/):

| Commit prefix | Bump |
|---|---|
| `feat!:` / any breaking `!` | major |
| `feat:` | minor |
| `fix:`, `chore:`, `docs:`, etc. | patch |

A manual bump can be triggered via **Actions → Release → Run workflow**.

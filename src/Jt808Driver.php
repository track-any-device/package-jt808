<?php

declare(strict_types=1);

namespace TrackAnyDevice\Jt808;

use TrackAnyDevice\Drivers\Contracts\DeviceDriverInterface;
use TrackAnyDevice\Drivers\ValueObjects\AddOnCommand;
use TrackAnyDevice\Drivers\ValueObjects\SignalObject;
use TrackAnyDevice\Core\Enums\DeviceCommandStatus;
use TrackAnyDevice\Core\Enums\SignalEventType;
use TrackAnyDevice\Core\Enums\SignalSource;
use TrackAnyDevice\Core\Enums\WorkingMode;
use TrackAnyDevice\Core\Models\Device;
use TrackAnyDevice\Core\Models\DeviceCommand;
use Carbon\CarbonImmutable;
use Illuminate\Support\Facades\Redis;

/**
 * Driver for JT/T 808-2019 GPS trackers (P901 and the broader family).
 *
 * Telemetry: device pushes binary frames over TCP to the Go jt808-server
 * (`jt808-server/`). The Go server decodes and publishes onto Redis Streams
 * (`jt808:telemetry`); `ConsumeJt808Stream` reads and feeds each event
 * through `parseEventToSignal()`.
 *
 * Outgoing commands: produced as JSON command descriptors and published to
 * `jt808:cmd:{phone}` for the Go server to encode and write to the socket.
 */
class Jt808Driver implements DeviceDriverInterface
{
    public function getStreamChannel(): string
    {
        return 'jt808';
    }

    public function supportsStream(Device $device): bool
    {
        return $device->last_signal_at !== null
            && $device->last_signal_at->isAfter(now()->subMinutes(10));
    }

    public function parseSmsToSignal(string $rawSms, Device $device): SignalObject
    {
        // Generic JT808 devices are reached over the TCP stream, not SMS.
        // If one ever sends an SMS to the platform, we capture it as a raw
        // heartbeat-style signal so it isn't silently dropped.
        return new SignalObject(
            eventType: SignalEventType::Update,
            source: SignalSource::GsmSms,
            rawPayload: $rawSms,
        );
    }

    public function parseEventToSignal(array $rawEvent, Device $device): SignalObject
    {
        $msgId = isset($rawEvent['msg_id']) ? (int) $rawEvent['msg_id'] : null;
        $alarmFlags = (int) ($rawEvent['alarm_flags'] ?? 0);
        $statusFlags = (int) ($rawEvent['status_flags'] ?? 0);
        $workingMode = WorkingMode::fromJt808Bits(($statusFlags >> 4) & 0b111)?->value;

        return new SignalObject(
            eventType: $this->resolveEventType($msgId, $alarmFlags),
            source: SignalSource::StreamJt808,
            latitude: isset($rawEvent['latitude']) ? (float) $rawEvent['latitude'] : null,
            longitude: isset($rawEvent['longitude']) ? (float) $rawEvent['longitude'] : null,
            altitude: isset($rawEvent['altitude']) ? (int) $rawEvent['altitude'] : null,
            speed: isset($rawEvent['speed']) ? (float) $rawEvent['speed'] : null,
            direction: isset($rawEvent['direction']) ? (int) $rawEvent['direction'] : null,
            gpsFixed: ! empty($rawEvent['gps_fixed']),
            satellites: isset($rawEvent['satellites']) ? (int) $rawEvent['satellites'] : null,
            batteryPercent: isset($rawEvent['battery_percent']) ? (int) $rawEvent['battery_percent'] : null,
            batteryVoltage: isset($rawEvent['battery_voltage']) ? (int) $rawEvent['battery_voltage'] : null,
            gsmSignal: isset($rawEvent['gsm_signal']) ? (int) $rawEvent['gsm_signal'] : null,
            networkSignal: isset($rawEvent['network_signal']) ? (int) $rawEvent['network_signal'] : null,
            mcc: isset($rawEvent['mcc']) ? (int) $rawEvent['mcc'] : null,
            mnc: isset($rawEvent['mnc']) ? (int) $rawEvent['mnc'] : null,
            lac: isset($rawEvent['lac']) ? (int) $rawEvent['lac'] : null,
            cellId: isset($rawEvent['cell_id']) ? (int) $rawEvent['cell_id'] : null,
            workingMode: $workingMode,
            alarmFlags: $alarmFlags,
            statusFlags: $statusFlags,
            temperature: isset($rawEvent['temperature']) ? (float) $rawEvent['temperature'] : null,
            rawPayload: $rawEvent['raw'] ?? null,
            extra: (array) ($rawEvent['extra'] ?? []),
            deviceTime: isset($rawEvent['device_time'])
                ? CarbonImmutable::parse((string) $rawEvent['device_time'])->utc()
                : null,
        );
    }

    public function requestSignal(string $signalType, Device $device): void
    {
        $this->publishStreamCommand($device, [
            'msg_id' => 0x8201,
            'type' => 'query_location',
        ]);
    }

    public function setMode(string $mode, Device $device, array $params = []): void
    {
        $this->publishStreamCommand($device, [
            'msg_id' => 0x8103,
            'type' => 'set_params',
            'mode' => $mode,
            'interval' => $params['interval'] ?? 30,
        ]);
    }

    public function getMode(Device $device): ?string
    {
        return $device->metadata['working_mode'] ?? null;
    }

    public function onboardingAction(Device $device): void
    {
        $apn = $device->gsmNetwork?->apn ?? 'internet';
        $host = (string) config('jt808.server_host', '');
        $port = (int) config('jt808.server_port', 7018);

        $this->publishStreamCommand($device, [
            'msg_id' => 0x8103,
            'type' => 'set_params',
            'host' => $host,
            'port' => $port,
            'apn' => $apn,
        ]);
    }

    public function addOnCommands(): array
    {
        return [
            new AddOnCommand('query_location', 'Query Location', [], 'tracking', false),
            new AddOnCommand('set_report_interval', 'Set Report Interval', ['seconds' => ['type' => 'integer', 'required' => true]], 'tracking', false),
            new AddOnCommand('set_heartbeat_interval', 'Set Heartbeat Interval', ['seconds' => ['type' => 'integer', 'required' => true]], 'tracking', false),
            new AddOnCommand('set_server', 'Set Server', ['host' => ['type' => 'string', 'required' => true], 'port' => ['type' => 'integer', 'required' => true]], 'network', false),
            new AddOnCommand('set_apn', 'Set APN', ['apn' => ['type' => 'string', 'required' => true]], 'network', false),
            new AddOnCommand('restart', 'Restart', [], 'utility', false),
            new AddOnCommand('factory_reset', 'Factory Reset', [], 'utility', false),
            new AddOnCommand('enable_acc', 'Enable ACC Detection', [], 'utility', false),
        ];
    }

    public function addOnCommand(string $commandName, array $parameters, Device $device): void
    {
        $descriptor = match ($commandName) {
            'query_location' => ['msg_id' => 0x8201, 'type' => 'query_location'],
            'set_report_interval' => ['msg_id' => 0x8103, 'type' => 'set_params', 'report_interval' => (int) ($parameters['seconds'] ?? 30)],
            'set_heartbeat_interval' => ['msg_id' => 0x8103, 'type' => 'set_params', 'heartbeat_interval' => (int) ($parameters['seconds'] ?? 60)],
            'set_server' => ['msg_id' => 0x8103, 'type' => 'set_params', 'host' => $parameters['host'] ?? '', 'port' => (int) ($parameters['port'] ?? 7018)],
            'set_apn' => ['msg_id' => 0x8103, 'type' => 'set_params', 'apn' => $parameters['apn'] ?? 'internet'],
            'restart' => ['msg_id' => 0x8105, 'type' => 'restart'],
            'factory_reset' => ['msg_id' => 0x8105, 'type' => 'factory_reset'],
            'enable_acc' => ['msg_id' => 0x8103, 'type' => 'set_params', 'enable_acc' => true],
            default => null,
        };

        if ($descriptor === null) {
            return;
        }

        // Save the command record only; Jt808Connector::send() is responsible
        // for publishing to Redis so the command is never sent twice.
        DeviceCommand::create([
            'device_id' => $device->id,
            'command_type' => $commandName,
            'command_payload' => json_encode($descriptor + ['params' => $parameters]),
            'channel' => 'jt808',
            'status' => DeviceCommandStatus::Pending,
            'requested_by' => auth()->id(),
        ]);
    }

    public function buildSmsBody(string $commandType, array $params): ?string
    {
        // JT808 devices are reached over the TCP stream, not SMS.
        return null;
    }

    // ── Internals ───────────────────────────────────────────────────────────

    private function resolveEventType(?int $msgId, int $alarmFlags): SignalEventType
    {
        if ($msgId === 0x0002) {
            return SignalEventType::Heartbeat;
        }
        if ($msgId === 0x0100) {
            return SignalEventType::Registration;
        }
        // JT808-2019 §28, Table 14 alarm bits:
        // bit 0 = emergency (SOS), bit 1 = overspeed, bit 2 = low battery,
        // bit 3 = power failure, bit 4 = vibration/impact.
        // Punch-in/out are delivered via msg_id 0x0900 custom extensions,
        // not the standard alarm bitmask.
        if ($alarmFlags & 0x01) {
            return SignalEventType::Sos;
        }

        return SignalEventType::Update;
    }

    private function publishStreamCommand(Device $device, array $descriptor): void
    {
        $phone = $device->gsm_number;

        if (! $phone) {
            return;
        }

        try {
            Redis::connection('jt808')->publish("jt808:cmd:{$phone}", json_encode($descriptor));
        } catch (\Throwable) {
            // Stream may not be configured in non-tcp environments; safe to swallow.
        }
    }
}

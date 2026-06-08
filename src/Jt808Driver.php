<?php

declare(strict_types=1);

namespace TrackAnyDevice\Jt808;

use Carbon\CarbonImmutable;
use Illuminate\Support\Facades\Redis;
use TrackAnyDevice\Core\Enums\SignalEventType;
use TrackAnyDevice\Core\Enums\SignalSource;
use TrackAnyDevice\Core\Enums\WorkingMode;
use TrackAnyDevice\Core\Models\Device;
use TrackAnyDevice\Drivers\Contracts\DeviceDriverInterface;
use TrackAnyDevice\Drivers\ValueObjects\AddOnCommand;
use TrackAnyDevice\Drivers\ValueObjects\SignalObject;

/**
 * Generic JT808 stream driver.
 *
 * Parses telemetry published by the Go server (server-jt808) to the
 * jt808:telemetry Redis Stream. Used as the default driver for any
 * device type that communicates via JT808 but lacks a device-specific
 * driver (e.g. ios_app, android_app connected via JT808 TCP).
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

    public function parseEventToSignal(array $rawEvent, Device $device): SignalObject
    {
        $source = $rawEvent['source'] ?? SignalSource::StreamJt808->value;

        if ($source === SignalSource::StreamJt808->value) {
            return $this->parseStreamEvent($rawEvent);
        }

        return new SignalObject(
            eventType: SignalEventType::Update,
            source: SignalSource::GsmSms,
            rawPayload: (string) ($rawEvent['raw'] ?? ''),
        );
    }

    public function parseSmsToSignal(string $rawSms, Device $device): SignalObject
    {
        return new SignalObject(
            eventType: SignalEventType::Update,
            source: SignalSource::GsmSms,
            rawPayload: $rawSms,
        );
    }

    public function requestSignal(string $signalType, Device $device): void
    {
        $this->publishStreamCommand($device, ['msg_id' => 0x8201, 'type' => 'query_location']);
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

    public function onboardingAction(Device $device): void {}

    public function addOnCommands(): array
    {
        return [
            new AddOnCommand('query_location', 'Query Location', [], 'tracking', false),
        ];
    }

    public function addOnCommand(string $commandName, array $parameters, Device $device): void
    {
        if ($commandName === 'query_location') {
            $this->publishStreamCommand($device, ['msg_id' => 0x8201, 'type' => 'query_location']);
        }
    }

    private function parseStreamEvent(array $event): SignalObject
    {
        $alarmFlags = (int) ($event['alarm_flags'] ?? 0);
        $statusFlags = (int) ($event['status_flags'] ?? 0);
        $workingMode = WorkingMode::fromJt808Bits(($statusFlags >> 4) & 0b111)?->value;

        $battery = isset($event['battery_level']) ? (int) $event['battery_level'] : null;
        $battery ??= isset($event['battery_from_flags']) ? (int) $event['battery_from_flags'] : null;

        $extra = [];
        if (isset($event['acc_on'])) {
            $extra['acc_on'] = (bool) $event['acc_on'];
        }
        if (isset($event['extras'])) {
            $decoded = json_decode((string) $event['extras'], true);
            if (is_array($decoded)) {
                $extra = array_merge($extra, $decoded);
            }
        }

        return new SignalObject(
            eventType: $this->jt808EventType($alarmFlags, $event['msg_id'] ?? null),
            source: SignalSource::StreamJt808,
            latitude: isset($event['latitude']) ? (float) $event['latitude'] : null,
            longitude: isset($event['longitude']) ? (float) $event['longitude'] : null,
            altitude: isset($event['altitude']) ? (int) $event['altitude'] : null,
            speed: isset($event['speed']) ? (float) $event['speed'] : null,
            direction: isset($event['direction']) ? (int) $event['direction'] : null,
            gpsFixed: ! empty($event['gps_fixed']),
            batteryPercent: $battery,
            gsmSignal: isset($event['signal_strength']) ? (int) $event['signal_strength'] : null,
            workingMode: $workingMode,
            alarmFlags: $alarmFlags,
            statusFlags: $statusFlags,
            extra: $extra,
            deviceTime: isset($event['timestamp'])
                ? CarbonImmutable::parse((string) $event['timestamp'])->utc()
                : null,
        );
    }

    private function jt808EventType(int $alarmFlags, mixed $msgId): SignalEventType
    {
        if ($msgId === 0x0002 || $msgId === '2') {
            return SignalEventType::Heartbeat;
        }
        if ($msgId === 0x0100 || $msgId === '256') {
            return SignalEventType::Registration;
        }
        if ($alarmFlags & 0b0001) {
            return SignalEventType::Sos;
        }
        if ($alarmFlags & 0b0010) {
            return SignalEventType::Alarm;
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
            // Stream not configured in this environment.
        }
    }
}

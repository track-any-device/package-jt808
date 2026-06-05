<?php

declare(strict_types=1);

namespace TrackAnyDevice\Jt808\Console\Commands;

use TrackAnyDevice\Core\Enums\DeviceStatus;
use TrackAnyDevice\Core\Enums\SignalSource;
use TrackAnyDevice\Core\Jobs\CheckBeatViolation;
use TrackAnyDevice\Core\Jobs\ProcessAlarmEvents;
use TrackAnyDevice\Core\Models\Device;
use TrackAnyDevice\Core\Models\DeviceType;
use TrackAnyDevice\Core\Providers\DeviceServiceProvider;
use TrackAnyDevice\Core\Services\SignalService;
use Illuminate\Console\Command;
use Illuminate\Redis\Connections\Connection;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;

/**
 * Consumes the jt808:telemetry Redis Stream published by the Go JT808 server.
 *
 * Every entry is normalised by Jt808Driver::parseEventToSignal() into a
 * SignalObject and persisted via SignalService — which writes the InfluxDB
 * point and updates the device snapshot in MySQL.
 */
class ConsumeJt808Stream extends Command
{
    protected $signature = 'jt808:consume
                            {--stream=jt808:telemetry : Redis stream key}
                            {--group=laravel : Consumer group name}
                            {--consumer= : Unique consumer name (defaults to hostname)}
                            {--batch=50 : Messages to read per XREADGROUP call}
                            {--block=2000 : Block timeout ms when stream is empty}';

    protected $description = 'Consume JT808 telemetry from Redis Stream and write signals.';

    private bool $running = true;

    private ?int $jt808TypeId = null;

    private ?Connection $redis = null;

    public function __construct(private readonly SignalService $signalService)
    {
        parent::__construct();
    }

    private function redis(): Connection
    {
        return $this->redis ??= Redis::connection('jt808');
    }

    public function handle(): int
    {
        $stream = $this->option('stream');
        $group = $this->option('group');
        $consumer = $this->option('consumer') ?: gethostname();
        $batch = (int) $this->option('batch');
        $block = (int) $this->option('block');

        if (function_exists('pcntl_signal')) {
            pcntl_signal(SIGTERM, fn () => $this->running = false);
            pcntl_signal(SIGINT, fn () => $this->running = false);
        }

        $this->ensureConsumerGroup($stream, $group);
        $this->info("JT808 stream consumer started — stream={$stream} group={$group} consumer={$consumer}");

        $this->processPending($stream, $group, $consumer, $batch);

        while ($this->running) {
            if (function_exists('pcntl_signal_dispatch')) {
                pcntl_signal_dispatch();
            }

            foreach ($this->readGroup($stream, $group, $consumer, $batch, $block) as $id => $fields) {
                try {
                    $this->processEntry($fields);
                    $this->ack($stream, $group, $id);
                } catch (\Throwable $e) {
                    report($e);
                    $this->error("Entry {$id} failed: {$e->getMessage()}");
                }
            }
        }

        $this->info('JT808 stream consumer stopped.');

        return self::SUCCESS;
    }

    // ── Entry dispatch ───────────────────────────────────────────────────────

    private function processEntry(array $fields): void
    {
        $phone = $fields['phone'] ?? null;

        if (! $phone) {
            return;
        }

        match ($fields['event'] ?? null) {
            'device.registered' => $this->handleRegistration($phone, $fields),
            'device.authenticated' => $this->handleAuthenticated($phone, $fields),
            'location' => $this->handleLocation($phone, $fields),
            default => null,
        };
    }

    private function handleRegistration(string $phone, array $fields): void
    {
        $payload = is_string($fields['payload'] ?? null)
            ? json_decode($fields['payload'], true) ?? []
            : [];

        $deviceId = $payload['device_id'] ?? '';
        $model = $payload['device_model'] ?? 'JT808';
        $imei = ($deviceId !== '' && strlen($deviceId) >= 7) ? $deviceId : $phone;

        // Find existing device by gsm_number, or by imei-prefix match so that
        // pre-seeded devices (e.g. imei=00000000000000) are linked rather than
        // duplicated when the JT808 phone field is a 12-char prefix of the IMEI.
        $existing = Device::where('gsm_number', $phone)->first()
            ?? Device::whereRaw('LEFT(imei, ?) = ?', [strlen($phone), $phone])->first();

        if ($existing) {
            if (! $existing->gsm_number) {
                $existing->forceFill(['gsm_number' => $phone])->save();
            }
        } else {
            Device::create([
                'gsm_number' => $phone,
                'device_type_id' => $this->jt808TypeId(),
                'imei' => $imei,
                'name' => trim("{$model} {$phone}"),
                'status' => DeviceStatus::Registration,
                'notes' => 'Auto-registered via JT808 TCP connection.',
                'metadata' => array_filter([
                    'device_model' => $model ?: null,
                    'province_id' => $payload['province_id'] ?? null,
                    'city_id' => $payload['city_id'] ?? null,
                    'protocol' => 'jt808',
                ]),
            ]);
        }

        Log::info('JT808 device registration', [
            'phone' => $phone,
            'imei' => $imei,
            'model' => $model,
        ]);
    }

    private function handleAuthenticated(string $phone, array $fields): void
    {
        $device = Device::where('gsm_number', $phone)->first()
            ?? Device::create([
                'device_type_id' => $this->jt808TypeId(),
                'gsm_number' => $phone,
                'imei' => $fields['imei'] ?? $phone,
                'name' => "JT808 {$phone}",
                'status' => DeviceStatus::Registration,
                'notes' => 'Auto-registered on authentication (no prior registration event).',
                'metadata' => ['protocol' => 'jt808'],
            ]);

        $loginAt = $fields['login_at'] ?? now()->toIso8601String();
        $device->forceFill(['last_seen_at' => $loginAt])->save();

        Log::info('JT808 device authenticated (online)', [
            'device_id' => $device->id,
            'phone' => $phone,
            'login_at' => $loginAt,
        ]);
    }

    private function handleLocation(string $phone, array $fields): void
    {
        $device = Device::with(['deviceType', 'driver'])->where('gsm_number', $phone)->first();

        if (! $device) {
            Log::info('JT808 location skipped: no device matched gsm_number', ['phone' => $phone]);
            return;
        }

        // Only record signals for devices that have been admin-approved.
        // Devices in Registration, Warehouse, or Inventory have not yet been
        // reviewed by central staff — signals are held until approval.
        $approvedStatuses = [
            DeviceStatus::Available,
            DeviceStatus::Assigned,
            DeviceStatus::InService,
            DeviceStatus::Maintenance,
            DeviceStatus::InTransit,
        ];

        if (! in_array($device->status, $approvedStatuses)) {
            Log::info('JT808 location skipped: device pending admin approval', [
                'device_id' => $device->id,
                'phone'     => $phone,
                'status'    => $device->status->value,
            ]);
            return;
        }

        $driver = DeviceServiceProvider::driverFor($device->deviceType->slug);

        $signalObject = $driver->parseEventToSignal(
            array_merge($fields, ['source' => SignalSource::StreamJt808->value]),
            $device,
        );

        $this->signalService->record($signalObject, $device);

        if ($signalObject->hasLocation()) {
            CheckBeatViolation::dispatch($device->id, (float) $signalObject->latitude, (float) $signalObject->longitude);
        }

        $activeAlarms = $this->resolveActiveAlarms($signalObject->alarmFlags ?? 0);

        ProcessAlarmEvents::dispatch(
            deviceId: $device->id,
            latitude: (float) ($signalObject->latitude ?? 0),
            longitude: (float) ($signalObject->longitude ?? 0),
            speedKmh: (float) ($signalObject->speed ?? 0),
            batteryLevel: $signalObject->batteryPercent,
            accOn: (bool) ($signalObject->extra['acc_on'] ?? false),
            activeAlarms: $activeAlarms,
        );
    }

    /** Translate the JT808 alarm bitmask into the symbolic alarm names ProcessAlarmEvents expects. */
    private function resolveActiveAlarms(int $flags): array
    {
        $alarms = [];

        if ($flags & 0x01) {
            $alarms[] = 'sos';
        }
        if ($flags & 0x02) {
            $alarms[] = 'overspeed';
        }
        if ($flags & 0x04) {
            $alarms[] = 'low_battery';
        }
        if ($flags & 0x08) {
            $alarms[] = 'power_failure';
        }
        if ($flags & 0x10) {
            $alarms[] = 'vibration';
        }

        return $alarms;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private function jt808TypeId(): int
    {
        return $this->jt808TypeId ??= DeviceType::where('stream_channel', 'jt808')->value('id')
            ?? throw new \RuntimeException('No device type with stream_channel=jt808 found. Run: php artisan db:seed --class=DeviceTypeSeeder');
    }

    private function ensureConsumerGroup(string $stream, string $group): void
    {
        try {
            $this->redis()->xgroup('CREATE', $stream, $group, '0-0', true);
        } catch (\Exception $e) {
            if (! str_contains($e->getMessage(), 'BUSYGROUP')) {
                throw $e;
            }
        }
    }

    /** @return array<string, array<string, string>> */
    private function readGroup(string $stream, string $group, string $consumer, int $count, int $block): array
    {
        $results = $this->redis()->xreadgroup($group, $consumer, [$stream => '>'], $count, $block);

        return $this->normalizeResult($results, $stream);
    }

    private function processPending(string $stream, string $group, string $consumer, int $batch): void
    {
        $cursor = '0-0';
        do {
            // xautoclaim returns a numeric array under Predis and an associative
            // array under PhpRedis — normalise both shapes.
            $raw = $this->redis()->xautoclaim($stream, $group, $consumer, 60_000, $cursor, $batch);
            $cursor  = is_array($raw) ? ($raw[0] ?? $raw['nextId'] ?? '0-0') : '0-0';
            $entries = is_array($raw) ? ($raw[1] ?? $raw['messages'] ?? []) : [];
            foreach ($entries as $id => $fields) {
                try {
                    $this->processEntry($fields);
                    $this->ack($stream, $group, $id);
                } catch (\Throwable $e) {
                    report($e);
                }
            }
        } while ($cursor !== '0-0');
    }

    private function ack(string $stream, string $group, string $id): void
    {
        $this->redis()->xack($stream, $group, [$id]);
    }

    private function normalizeResult(mixed $results, string $stream): array
    {
        if (! is_array($results)) {
            return [];
        }

        $entries = $results[$stream] ?? null;

        return is_array($entries) ? $entries : [];
    }
}

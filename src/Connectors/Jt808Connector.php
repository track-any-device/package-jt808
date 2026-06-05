<?php

declare(strict_types=1);

namespace TrackAnyDevice\Jt808\Connectors;

use TrackAnyDevice\Drivers\Contracts\DeviceConnectorInterface;
use TrackAnyDevice\Core\Enums\DeviceCommandStatus;
use TrackAnyDevice\Core\Models\Device;
use TrackAnyDevice\Core\Models\DeviceCommand;
use Illuminate\Support\Facades\Redis;

/**
 * JT808 connector — routes outgoing commands to the Go JT808 server via Redis pub/sub.
 *
 * Command flow:
 *   Laravel Filament / API → DeviceCommand queued → Jt808Connector::send()
 *   → PUBLISH jt808:cmd:{phone} {command_json}
 *   → Go server subscribes → builds binary JT808 frame → writes to device TCP socket
 *
 * Why pub/sub (not Streams):
 *   Commands are fire-and-forget in nature — the Go server must process them
 *   while the device is connected. If the device is offline, the command cannot
 *   be delivered anyway, so persistence adds no value. Pub/sub is simpler here.
 *   If you need guaranteed delivery with retry, store the command in MySQL and
 *   have the Go server check for pending commands on auth.
 *
 * The `message` argument is a JSON command descriptor produced by the device-type driver (e.g. P901Driver).
 */
class Jt808Connector implements DeviceConnectorInterface
{
    public function send(Device $device, DeviceCommand $command, string $message): void
    {
        // JT808 devices are identified by their phone number (BCD-encoded in the protocol).
        // The gsm_number field stores this — it's admin-only per privacy rules.
        $phone = $device->gsm_number;

        if (! $phone) {
            $command->update([
                'status' => DeviceCommandStatus::Failed,
                'failed_reason' => 'Device has no GSM number configured.',
            ]);

            return;
        }

        $channel = 'jt808:cmd:'.$phone;

        // Decorate the command JSON with metadata the Go server needs.
        $payload = json_encode([
            'command_id' => $command->id,
            'phone' => $phone,
            'imei' => $device->imei,
            'payload' => json_decode($message, true),
            'issued_at' => now()->toIso8601String(),
        ], JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);

        $subscriberCount = Redis::connection('jt808')->publish($channel, $payload);

        if ($subscriberCount === 0) {
            // No Go server replica is currently subscribed → device is offline
            // or the jt808-server is down. Mark as queued; retry externally.
            $command->update([
                'status' => DeviceCommandStatus::Queued,
                'command_payload' => $message,
                'failed_reason' => 'No active JT808 server replica received the command (device may be offline).',
            ]);
        } else {
            $command->update([
                'status' => DeviceCommandStatus::Sent,
                'command_payload' => $message,
                'sent_at' => now(),
            ]);
        }
    }
}

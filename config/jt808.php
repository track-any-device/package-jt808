<?php

return [
    // JT808 TCP server hostname written into device setup SMS (adminip command).
    // Defaults to JT808_HOST env var; falls back to APP_DOMAIN.
    'server_host' => env('JT808_HOST', env('APP_DOMAIN', '')),

    // JT808 TCP port written into device setup SMS.
    'server_port' => (int) env('JT808_PORT', 7018),

    // UTC offset sent to devices via the timezone SMS command.
    // Pakistan Standard Time = 5 (UTC+5). Negative values for west timezones.
    'timezone' => (int) env('JT808_TIMEZONE', 5),
];

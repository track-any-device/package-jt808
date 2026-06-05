<?php

namespace TrackAnyDevice\Jt808;

use Illuminate\Support\ServiceProvider;
use TrackAnyDevice\Jt808\Console\Commands\ConsumeJt808Stream;

class Jt808ServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/jt808.php', 'jt808');
    }

    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/../config/jt808.php' => config_path('jt808.php'),
            ], 'jt808-config');

            $this->commands([
                ConsumeJt808Stream::class,
            ]);
        }
    }
}

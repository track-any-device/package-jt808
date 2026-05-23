<?php

namespace TrackAnyDevice\Jt808;

use Illuminate\Support\ServiceProvider;
use TrackAnyDevice\Jt808\Console\Commands\ConsumeJt808Stream;

class Jt808ServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        //
    }

    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->commands([
                ConsumeJt808Stream::class,
            ]);
        }
    }
}

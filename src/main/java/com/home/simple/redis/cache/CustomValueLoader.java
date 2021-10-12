package com.home.simple.redis.cache;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Function;

public class CustomValueLoader<V> {

    private final Future<V> futureValue;
    private final ScheduledFuture<?> lockRefresher;

    public CustomValueLoader(Callable<V> valueLoader,
                             ExecutorService backgroundExecutor,
                             ScheduledExecutorService scheduleAtFixedRate,
                             Runnable refreshFunction,
                             Duration lockTtl) {
        this.futureValue = backgroundExecutor.submit(valueLoader);

        this.lockRefresher = scheduleAtFixedRate.scheduleAtFixedRate(() -> {
            if (!futureValue.isDone()) {
                refreshFunction.run();
            }
        }, lockTtl.getSeconds(), lockTtl.getSeconds(), TimeUnit.SECONDS);
    }

    public V call() throws Exception {
        try {
            return futureValue.get();
        } finally {
            this.lockRefresher.cancel(true);
        }
    }
}

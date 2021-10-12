package com.home.simple.redis.cache;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CustomPubSub extends BinaryJedisPubSub {

    private final JedisPool jedisPool;
    private final ScheduledFuture<?> lockChecker;

    public CustomPubSub(JedisPool jedisPool, ScheduledExecutorService scheduleAtFixedRate, byte[] key, Duration lockTtl) {
        this.jedisPool = jedisPool;

        lockChecker = scheduleAtFixedRate.scheduleAtFixedRate(() -> {
            if (!checkLock(key)) {
                unsubscribe();
            }
        }, lockTtl.getSeconds(), lockTtl.getSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void onMessage(byte[] channel, byte[] message) {
        log.debug("Received message. Unsubscribing - {}", message);
        lockChecker.cancel(true);
        this.unsubscribe();
    }

    private Boolean checkLock(byte[] key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(key);
        }
    }

}

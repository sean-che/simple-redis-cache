package com.home.simple.redis.cache;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
@RequiredArgsConstructor
public class SimpleRedisWriter {

    private final JedisPool jedisPool;
    private final ScheduledExecutorService scheduledExecutor;

    public byte[] get(byte[] key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        }
    }

    public Long del(byte[] key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.del(key);
        }
    }

    public String setex(byte[] key, Duration ttl, byte[] value) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.setex(key, ttl.getSeconds(), value);
        }
    }

    // Implementation to set lock key which will use current timestamp as a value.
    public String acquireLock(byte[] key, Duration lockTtl) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.set(key, LocalDateTime.now().toString().getBytes(StandardCharsets.UTF_8),
                    SetParams.setParams().nx().ex(lockTtl.getSeconds()));
        }
    }

    public Long refreshLock(byte[] key, Duration lockTtl) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.expire(key, lockTtl.getSeconds());
        }
    }

    public void releaseLock(byte[] key, byte[] message) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (del(key) <= 0) {
                log.error("Unsuccessful removal of lock record - {}", key);
            }
            if (jedis.publish(key, message) <= 0) {
                log.error("Unsuccessful publish - {}", key);
            }
        }
    }

    public void subscribeAndWait(byte[] key, Duration lockTtl) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.subscribe(new CustomPubSub(jedisPool, scheduledExecutor, key, lockTtl), key);
        }
    }

}

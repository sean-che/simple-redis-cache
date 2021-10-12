package com.home.simple.redis.cache;

import org.springframework.cache.support.AbstractValueAdaptingCache;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.*;

public class SimpleRedisCache extends AbstractValueAdaptingCache {

    private final SimpleRedisWriter simpleRedisWriter;
    private final String cacheName;
    private final SimpleSerializer simpleSerializer;
    private final SimpleRedisCacheConfig simpleRedisCacheConfig;
    private final ExecutorService backgroundExecutor;
    private final ScheduledExecutorService scheduledExecutor;

    protected SimpleRedisCache(boolean allowNullValues,
                               SimpleRedisWriter simpleRedisWriter,
                               String cacheName,
                               SimpleSerializer simpleSerializer,
                               SimpleRedisCacheConfig simpleRedisCacheConfig,
                               ExecutorService backgroundExecutor,
                               ScheduledExecutorService scheduledExecutor) {
        super(allowNullValues);
        this.simpleRedisWriter = simpleRedisWriter;
        this.cacheName = cacheName;
        this.simpleSerializer = simpleSerializer;
        this.simpleRedisCacheConfig = simpleRedisCacheConfig;
        this.backgroundExecutor = backgroundExecutor;
        this.scheduledExecutor = scheduledExecutor;
    }

    @Override
    protected Object lookup(Object key) {
        byte[] value = simpleRedisWriter.get(prepareKey(key));
        if (isNull(value)) {
            return null;
        }
        return simpleSerializer.deserializeValue(value, simpleRedisCacheConfig.getCacheType());
    }

    @Override
    public String getName() {
        return this.cacheName;
    }

    @Override
    public SimpleRedisWriter getNativeCache() {
        return this.simpleRedisWriter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Callable<T> valueLoader) {
        ValueWrapper result = get(key);

        if (nonNull(result)) {
            return (T) result.get();
        }

        if ("OK".equals(simpleRedisWriter.acquireLock(prepareLockKey(key), simpleRedisCacheConfig.getLockTtl()))) {
            return putInternal(key, valueLoader);
        } else {
            simpleRedisWriter.subscribeAndWait(prepareLockKey(key), simpleRedisCacheConfig.getLockTtl()); // Wait for value from another thread
            // ToDo: SC: Check if nested call. Should be called only once from this place.
            return get(key, valueLoader);
        }

    }

    private <T> T putInternal(final Object key, Callable<T> valueLoader) {
        ThreadLocal<T> value = new ThreadLocal<>();
        try {
            try {
                value.set(new CustomValueLoader<T>(valueLoader,
                        backgroundExecutor, scheduledExecutor,
                        () -> {
                            simpleRedisWriter.refreshLock(prepareLockKey(key), simpleRedisCacheConfig.getLockTtl());
                        },
                        simpleRedisCacheConfig.getLockRefresh()).call());
                put(key, value.get());
                simpleRedisWriter.releaseLock(prepareLockKey(key), "OK".getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                simpleRedisWriter.releaseLock(prepareLockKey(key), "ERROR".getBytes(StandardCharsets.UTF_8));
                throw new ValueRetrievalException(key, valueLoader, e);
            }
            return value.get();
        } finally {
            value.remove();
        }
    }

    @Override
    public void put(Object key, Object value) {
        // ToDo: SC: check the result
        simpleRedisWriter.setex(prepareKey(key), simpleRedisCacheConfig.getTtl(), prepareValue(value));
    }

    @Override
    public void evict(Object key) {
        // ToDo: SC: check the result
        simpleRedisWriter.del(prepareKey(key));
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private byte[] prepareKey(Object key) {
        requireNonNull(key, "key cannot be null");
        return simpleSerializer.serializeKey(simpleRedisCacheConfig.getKeyPrefix(), key);
    }

    private byte[] prepareLockKey(Object key) {
        requireNonNull(key, "key cannot be null");
        return simpleSerializer.serializeKey(simpleRedisCacheConfig.getLockKeyPrefix(), key);
    }

    private byte[] prepareValue(Object value) {
        return simpleSerializer.serializeValue(value);
    }

}

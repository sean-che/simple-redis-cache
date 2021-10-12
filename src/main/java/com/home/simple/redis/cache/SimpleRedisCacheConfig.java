package com.home.simple.redis.cache;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.Duration;

@Getter
@RequiredArgsConstructor
public class SimpleRedisCacheConfig {

    private final Duration ttl;
    private final Duration lockTtl;
    private final Duration lockRefresh;

    private final Class<?> cacheType;
    private final String keyPrefix;
    private final String lockKeyPrefix;

}

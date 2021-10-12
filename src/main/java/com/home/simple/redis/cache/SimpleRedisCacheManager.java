package com.home.simple.redis.cache;

import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractCacheManager;

import java.util.Collection;

public class SimpleRedisCacheManager extends AbstractCacheManager {


    @Override
    protected Collection<? extends Cache> loadCaches() {
        return null;
    }

    public class SimpleRedisCacheManagerBuilder {

    }
}

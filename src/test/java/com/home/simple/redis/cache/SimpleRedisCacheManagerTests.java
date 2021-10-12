package com.home.simple.redis.cache;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cache.Cache;
import org.springframework.cache.transaction.TransactionAwareCacheDecorator;
import org.springframework.data.redis.cache.RedisCacheManager.RedisCacheManagerBuilder;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SimpleRedisCacheManagerTests {

    @Mock
    RedisCacheWriter cacheWriter;

    @Test
        // DATAREDIS-481
    void missingCacheShouldBeCreatedWithDefaultConfiguration() {

        RedisCacheConfiguration configuration = RedisCacheConfiguration.defaultCacheConfig().disableKeyPrefix();

        RedisCacheManager cm = RedisCacheManager.builder(cacheWriter).cacheDefaults(configuration).build();
        cm.afterPropertiesSet();

        assertThat(cm.getMissingCache("new-cache").getCacheConfiguration()).isEqualTo(configuration);
    }

    @Test
        // DATAREDIS-481
    void appliesDefaultConfigurationToInitialCache() {

        RedisCacheConfiguration withPrefix = RedisCacheConfiguration.defaultCacheConfig().disableKeyPrefix();
        RedisCacheConfiguration withoutPrefix = RedisCacheConfiguration.defaultCacheConfig().disableKeyPrefix();

        RedisCacheManager cm = RedisCacheManager.builder(cacheWriter).cacheDefaults(withPrefix) //
                .initialCacheNames(Collections.singleton("first-cache")) //
                .cacheDefaults(withoutPrefix) //
                .initialCacheNames(Collections.singleton("second-cache")) //
                .build();

        cm.afterPropertiesSet();

        assertThat(((RedisCache) cm.getCache("first-cache")).getCacheConfiguration()).isEqualTo(withPrefix);
        assertThat(((RedisCache) cm.getCache("second-cache")).getCacheConfiguration()).isEqualTo(withoutPrefix);
        assertThat(((RedisCache) cm.getCache("other-cache")).getCacheConfiguration()).isEqualTo(withoutPrefix);
    }

    @Test
        // DATAREDIS-481, DATAREDIS-728
    void predefinedCacheShouldBeCreatedWithSpecificConfig() {

        RedisCacheConfiguration configuration = RedisCacheConfiguration.defaultCacheConfig().disableKeyPrefix();

        RedisCacheManager cm = RedisCacheManager.builder(cacheWriter)
                .withInitialCacheConfigurations(Collections.singletonMap("predefined-cache", configuration))
                .withInitialCacheConfigurations(Collections.singletonMap("another-predefined-cache", configuration)).build();
        cm.afterPropertiesSet();

        assertThat(((RedisCache) cm.getCache("predefined-cache")).getCacheConfiguration()).isEqualTo(configuration);
        assertThat(((RedisCache) cm.getCache("another-predefined-cache")).getCacheConfiguration()).isEqualTo(configuration);
        assertThat(cm.getMissingCache("new-cache").getCacheConfiguration()).isNotEqualTo(configuration);
    }

    @Test
        // DATAREDIS-481
    void transactionAwareCacheManagerShouldDecoracteCache() {

        Cache cache = RedisCacheManager.builder(cacheWriter).transactionAware().build().getCache("decoracted-cache");

        assertThat(cache).isInstanceOfAny(TransactionAwareCacheDecorator.class);
        assertThat(ReflectionTestUtils.getField(cache, "targetCache")).isInstanceOf(RedisCache.class);
    }

    @Test
        // DATAREDIS-767
    void lockedCacheManagerShouldPreventInFlightCacheCreation() {

        RedisCacheManager cacheManager = RedisCacheManager.builder(cacheWriter).disableCreateOnMissingCache().build();
        cacheManager.afterPropertiesSet();

        assertThat(cacheManager.getCache("not-configured")).isNull();
    }

    @Test
        // DATAREDIS-767
    void lockedCacheManagerShouldStillReturnPreconfiguredCaches() {

        RedisCacheManager cacheManager = RedisCacheManager.builder(cacheWriter)
                .initialCacheNames(Collections.singleton("configured")).disableCreateOnMissingCache().build();
        cacheManager.afterPropertiesSet();

        assertThat(cacheManager.getCache("configured")).isNotNull();
    }

    @Test
        // DATAREDIS-935
    void cacheManagerBuilderReturnsConfiguredCaches() {

        RedisCacheManagerBuilder cmb = RedisCacheManager.builder(cacheWriter)
                .initialCacheNames(Collections.singleton("configured")).disableCreateOnMissingCache();

        assertThat(cmb.getConfiguredCaches()).containsExactly("configured");
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> cmb.getConfiguredCaches().add("another"));
    }

    @Test
        // DATAREDIS-935
    void cacheManagerBuilderDoesNotAllowSneakingInConfiguration() {

        RedisCacheManagerBuilder cmb = RedisCacheManager.builder(cacheWriter)
                .initialCacheNames(Collections.singleton("configured")).disableCreateOnMissingCache();

        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> cmb.getConfiguredCaches().add("another"));
    }

    @Test
        // DATAREDIS-935
    void cacheManagerBuilderReturnsConfigurationForKnownCache() {

        RedisCacheManagerBuilder cmb = RedisCacheManager.builder(cacheWriter)
                .initialCacheNames(Collections.singleton("configured")).disableCreateOnMissingCache();

        assertThat(cmb.getCacheConfigurationFor("configured")).isPresent();
    }

    @Test
        // DATAREDIS-935
    void cacheManagerBuilderReturnsEmptyOptionalForUnknownCache() {

        RedisCacheManagerBuilder cmb = RedisCacheManager.builder(cacheWriter)
                .initialCacheNames(Collections.singleton("configured")).disableCreateOnMissingCache();

        assertThat(cmb.getCacheConfigurationFor("unknown")).isNotPresent();
    }

    @Test
        // DATAREDIS-1118
    void shouldConfigureRedisCacheWriter() {

        RedisCacheWriter writerMock = mock(RedisCacheWriter.class);

        RedisCacheManager cm = RedisCacheManager.builder(cacheWriter).cacheWriter(writerMock).build();

        assertThat(cm).extracting("cacheWriter").isEqualTo(writerMock);
    }

    @Test
        // DATAREDIS-1118
    void cacheWriterMustNotBeNull() {
        assertThatIllegalArgumentException().isThrownBy(() -> RedisCacheManager.builder().cacheWriter(null));
    }

    @Test
        // DATAREDIS-1118
    void builderShouldRequireCacheWriter() {
        assertThatIllegalStateException().isThrownBy(() -> RedisCacheManager.builder().build());
    }

    @Test
        // DATAREDIS-1082
    void builderSetsStatisticsCollectorWhenEnabled() {

        when(cacheWriter.withStatisticsCollector(any())).thenReturn(cacheWriter);
        RedisCacheManager.builder(cacheWriter).enableStatistics().build();

        verify(cacheWriter).withStatisticsCollector(any(DefaultCacheStatisticsCollector.class));
    }

    @Test
        // DATAREDIS-1082
    void builderWontSetStatisticsCollectorByDefault() {

        RedisCacheManager.builder(cacheWriter).build();

        verify(cacheWriter, never()).withStatisticsCollector(any());
    }


}

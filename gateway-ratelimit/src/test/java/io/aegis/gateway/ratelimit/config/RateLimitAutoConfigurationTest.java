package io.aegis.gateway.ratelimit.config;

import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.ratelimit.core.RedissonClientManager;
import io.aegis.gateway.ratelimit.filter.RateLimitFilter;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class RateLimitAutoConfigurationTest {

    @Test
    void autoConfigurationShouldRegisterFilterWithoutAnyRedissonBean() {
        // 关键约束：容器里没有 Redisson bean、Redis 未运行时也必须正常装配——
        // Redis 客户端由 RedissonClientManager 按治理配置惰性创建，启动不依赖 Redis
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RateLimitAutoConfiguration.class))
                .withBean(NacosConfigSyncService.class, () -> mock(NacosConfigSyncService.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .run(context -> {
                    assertThat(context).hasSingleBean(RateLimitFilter.class);
                    assertThat(context).hasSingleBean(RedissonClientManager.class);
                });
    }

    @Test
    void autoConfigurationShouldBackOffWhenNacosConfigSyncServiceIsMissing() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RateLimitAutoConfiguration.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .run(context -> assertThat(context).doesNotHaveBean(RateLimitFilter.class));
    }
}

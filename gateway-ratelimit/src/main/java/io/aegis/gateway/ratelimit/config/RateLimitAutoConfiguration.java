package io.aegis.gateway.ratelimit.config;

import io.aegis.gateway.core.config.AegisCoreAutoConfiguration;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.ratelimit.core.ReactiveRateLimiterGateway;
import io.aegis.gateway.ratelimit.core.RedissonClientManager;
import io.aegis.gateway.ratelimit.core.RedissonReactiveRateLimiterGateway;
import io.aegis.gateway.ratelimit.filter.RateLimitFilter;
import io.aegis.gateway.ratelimit.repository.RateLimitPolicyRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.ObjectMapper;

/**
 * gateway-ratelimit 模块的自动配置入口。
 * <p>
 * 刻意不以 Redisson 客户端 bean 为条件：Redis 客户端由 {@link RedissonClientManager}
 * 按治理配置惰性创建，网关启动既不要求 Redis 可用，也不要求 Spring 容器里存在
 * Redisson bean——未配置限流策略时本模块除注册几个轻量 bean 外没有任何运行时开销。
 */
@AutoConfiguration(after = AegisCoreAutoConfiguration.class)
@ConditionalOnClass(GlobalFilter.class)
@ConditionalOnBean(NacosConfigSyncService.class)
public class RateLimitAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public RedissonClientManager redissonClientManager() {
        return new RedissonClientManager();
    }

    @Bean
    @ConditionalOnMissingBean
    public RateLimitPolicyRepository rateLimitPolicyRepository(NacosConfigSyncService syncService,
                                                               ObjectMapper objectMapper,
                                                               RedissonClientManager clientManager) {
        return new RateLimitPolicyRepository(syncService, objectMapper, clientManager);
    }

    @Bean
    @ConditionalOnMissingBean
    public ReactiveRateLimiterGateway reactiveRateLimiterGateway(RedissonClientManager clientManager) {
        return new RedissonReactiveRateLimiterGateway(clientManager);
    }

    @Bean
    @ConditionalOnMissingBean
    public RateLimitFilter rateLimitFilter(RateLimitPolicyRepository repository,
                                           ReactiveRateLimiterGateway limiterGateway,
                                           ObjectMapper objectMapper) {
        return new RateLimitFilter(repository, limiterGateway, objectMapper);
    }
}

package io.aegis.gateway.core.config;

import com.alibaba.cloud.nacos.NacosConfigManager;
import io.aegis.gateway.core.exception.GlobalExceptionHandler;
import io.aegis.gateway.core.nacos.NacosConfigKeys;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.core.route.AegisRouteDefinitionRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.cloud.gateway.config.GatewayAutoConfiguration;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.ObjectMapper;

/**
 * gateway-core 模块的自动配置入口，注册 Nacos 配置同步、路由仓库、全局异常处理器三个核心 Bean。
 * <p>
 * 必须在 {@link GatewayAutoConfiguration} 之前执行，以确保 {@link AegisRouteDefinitionRepository}
 * 先于 SCG 默认的 RouteDefinitionRepository 注册，成为实际使用的实现。
 */
@AutoConfiguration(before = GatewayAutoConfiguration.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
public class AegisCoreAutoConfiguration {

    @Value("${aegis.gateway.nacos.group:" + NacosConfigKeys.DEFAULT_GROUP + "}")
    private String nacosGroup;

    @Bean
    @ConditionalOnMissingBean
    public NacosConfigSyncService nacosConfigSyncService(NacosConfigManager nacosConfigManager,
                                                         ObjectMapper objectMapper) throws Exception {
        return new NacosConfigSyncService(
                nacosConfigManager.getConfigService(), objectMapper, nacosGroup);
    }

    @Bean
    @ConditionalOnMissingBean(AegisRouteDefinitionRepository.class)
    public AegisRouteDefinitionRepository aegisRouteDefinitionRepository(
            NacosConfigSyncService syncService,
            ApplicationEventPublisher publisher) {
        return new AegisRouteDefinitionRepository(syncService, publisher);
    }

    @Bean
    @ConditionalOnMissingBean
    public GlobalExceptionHandler globalExceptionHandler(ObjectMapper objectMapper) {
        return new GlobalExceptionHandler(objectMapper);
    }
}

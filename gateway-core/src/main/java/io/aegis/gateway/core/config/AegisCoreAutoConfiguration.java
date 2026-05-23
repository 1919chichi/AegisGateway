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

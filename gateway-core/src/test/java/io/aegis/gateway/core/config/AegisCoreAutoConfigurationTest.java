package io.aegis.gateway.core.config;

import com.alibaba.cloud.nacos.NacosConfigManager;
import com.alibaba.nacos.api.config.ConfigService;
import io.aegis.gateway.core.nacos.NacosConfigKeys;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ReactiveWebApplicationContextRunner;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AegisCoreAutoConfigurationTest {

    private final ReactiveWebApplicationContextRunner contextRunner =
            new ReactiveWebApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(AegisCoreAutoConfiguration.class))
                    .withBean(ObjectMapper.class, ObjectMapper::new)
                    .withBean(NacosConfigManager.class, this::nacosConfigManager);

    @Test
    void autoConfiguration_shouldUseBoot4JacksonObjectMapper() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(NacosConfigSyncService.class);
        });
    }

    @Test
    void aegisRouteDefinitionRepository_shouldNotBePrimaryRouteDefinitionLocator() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBeanFactory()
                    .getBeanDefinition("aegisRouteDefinitionRepository")
                    .isPrimary()).isFalse();
        });
    }

    private NacosConfigManager nacosConfigManager() {
        try {
            ConfigService configService = mock(ConfigService.class);
            when(configService.getConfig(eq(NacosConfigKeys.ROUTES), eq(NacosConfigKeys.DEFAULT_GROUP), anyLong()))
                    .thenReturn("{\"routes\":[]}");
            when(configService.getConfig(eq(NacosConfigKeys.GOVERNANCE), eq(NacosConfigKeys.DEFAULT_GROUP), anyLong()))
                    .thenReturn("{}");
            when(configService.getConfig(eq(NacosConfigKeys.GLOBAL), eq(NacosConfigKeys.DEFAULT_GROUP), anyLong()))
                    .thenReturn("""
                            {
                              "cors": {"allowedOrigins": ["*"], "allowedMethods": ["GET"]},
                              "auth": {"jwtSecret": "", "excludePaths": []},
                              "admin": {"apiKey": ""}
                            }
                            """);

            NacosConfigManager manager = mock(NacosConfigManager.class);
            when(manager.getConfigService()).thenReturn(configService);
            return manager;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create NacosConfigManager mock", e);
        }
    }
}

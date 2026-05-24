package io.aegis.gateway.gray.config;

import io.aegis.gateway.core.model.config.AegisRoutesConfig;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.gray.filter.GrayRoutingFilter;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GrayAutoConfigurationTest {

    @Test
    void autoConfiguration_shouldRegisterGrayRoutingFilter() {
        NacosConfigSyncService mockSyncService = mock(NacosConfigSyncService.class);
        when(mockSyncService.getGovernanceConfigJson()).thenReturn("{}");
        when(mockSyncService.getRoutesConfig()).thenReturn(AegisRoutesConfig.empty());

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(GrayAutoConfiguration.class))
                .withBean(NacosConfigSyncService.class, () -> mockSyncService)
                .withBean(ObjectMapper.class, tools.jackson.databind.ObjectMapper::new)
                .run(context -> assertThat(context).hasSingleBean(GrayRoutingFilter.class));
    }

    @Test
    void autoConfiguration_shouldRespectCustomGovernanceKey() {
        NacosConfigSyncService mockSyncService = mock(NacosConfigSyncService.class);
        when(mockSyncService.getGovernanceConfigJson()).thenReturn("{}");
        when(mockSyncService.getRoutesConfig()).thenReturn(AegisRoutesConfig.empty());

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(GrayAutoConfiguration.class))
                .withBean(NacosConfigSyncService.class, () -> mockSyncService)
                .withBean(ObjectMapper.class, tools.jackson.databind.ObjectMapper::new)
                .withPropertyValues("spring.aegis.gray.governance-key=canary")
                .run(context -> assertThat(context).hasSingleBean(GrayRoutingFilter.class));
    }

    @Test
    void autoConfiguration_shouldNotRegisterFilterWhenNacosConfigSyncServiceIsMissing() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(GrayAutoConfiguration.class))
                .withBean(ObjectMapper.class, tools.jackson.databind.ObjectMapper::new)
                // NacosConfigSyncService NOT registered → @ConditionalOnBean should prevent filter
                .run(context -> assertThat(context).doesNotHaveBean(GrayRoutingFilter.class));
    }

    @Test
    void autoConfiguration_shouldNotOverrideUserProvidedGrayRoutingFilter() {
        NacosConfigSyncService mockSyncService = mock(NacosConfigSyncService.class);
        when(mockSyncService.getGovernanceConfigJson()).thenReturn("{}");
        when(mockSyncService.getRoutesConfig()).thenReturn(AegisRoutesConfig.empty());

        GrayRoutingFilter customFilter = new GrayRoutingFilter("custom", new tools.jackson.databind.ObjectMapper(), mockSyncService);

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(GrayAutoConfiguration.class))
                .withBean(NacosConfigSyncService.class, () -> mockSyncService)
                .withBean(ObjectMapper.class, tools.jackson.databind.ObjectMapper::new)
                .withBean(GrayRoutingFilter.class, () -> customFilter)
                // User-provided bean → @ConditionalOnMissingBean should skip auto-configured one
                .run(context -> {
                    assertThat(context).hasSingleBean(GrayRoutingFilter.class);
                    assertThat(context.getBean(GrayRoutingFilter.class)).isSameAs(customFilter);
                });
    }
}

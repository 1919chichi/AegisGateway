package io.aegis.gateway.gray.config;

import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.gray.filter.GrayRoutingFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.ObjectMapper;

/**
 * gateway-gray 模块的自动配置入口，注册 {@link GrayRoutingFilter} Bean。
 * <p>
 * 通过 {@code spring.aegis.gray.governance-key}（默认 {@code "gray"}）指定
 * {@code aegis-governance.json} 中灰度规则节点的 key，允许用户使用语义化名称（如 canary、staging）。
 */
@AutoConfiguration
@ConditionalOnClass(GlobalFilter.class)
@ConditionalOnBean(NacosConfigSyncService.class)
public class GrayAutoConfiguration {

    @Value("${spring.aegis.gray.governance-key:gray}")
    private String governanceKey;

    @Bean
    @ConditionalOnMissingBean
    public GrayRoutingFilter grayRoutingFilter(NacosConfigSyncService syncService,
                                               ObjectMapper objectMapper) {
        return new GrayRoutingFilter(governanceKey, objectMapper, syncService);
    }
}

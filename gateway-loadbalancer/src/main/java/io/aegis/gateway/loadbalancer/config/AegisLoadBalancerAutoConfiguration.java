package io.aegis.gateway.loadbalancer.config;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.cloud.nacos.loadbalancer.LoadBalancerNacosAutoConfiguration;
import com.alibaba.nacos.api.naming.NamingService;
import io.aegis.gateway.loadbalancer.discovery.NacosNamingServiceRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.loadbalancer.annotation.LoadBalancerClients;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.context.annotation.Bean;

/**
 * gateway-loadbalancer 模块的自动配置入口。
 * <p>
 * 注册 {@link NacosNamingServiceRegistry} Bean，并通过 {@code @LoadBalancerClients}
 * 将 {@link AegisNamespaceLoadBalancerClientConfiguration} 设为所有 LoadBalancer 客户端的
 * 默认配置，使命名空间感知的 Supplier 生效。
 * <p>
 * 在 {@link LoadBalancerNacosAutoConfiguration} 之后运行，确保 Nacos 相关 Bean 已就绪。
 */
@AutoConfiguration(after = LoadBalancerNacosAutoConfiguration.class)
@ConditionalOnClass({ NamingService.class, ServiceInstanceListSupplier.class })
@ConditionalOnBean(NacosDiscoveryProperties.class)
@LoadBalancerClients(defaultConfiguration = AegisNamespaceLoadBalancerClientConfiguration.class)
public class AegisLoadBalancerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public NacosNamingServiceRegistry nacosNamingServiceRegistry(NacosDiscoveryProperties discoveryProperties) {
        return new NacosNamingServiceRegistry(discoveryProperties);
    }
}

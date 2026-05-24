package io.aegis.gateway.loadbalancer.config;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import io.aegis.gateway.loadbalancer.discovery.NamespaceAwareNacosServiceInstanceListSupplier;
import io.aegis.gateway.loadbalancer.discovery.NacosNamingServiceRegistry;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.cloud.loadbalancer.support.LoadBalancerClientFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;

/**
 * 每个 LoadBalancer 客户端的配置类，为对应 serviceId 创建
 * {@link NamespaceAwareNacosServiceInstanceListSupplier}。
 * <p>
 * 由 {@link AegisLoadBalancerAutoConfiguration} 通过 {@code @LoadBalancerClients(defaultConfiguration)}
 * 注册为全局默认配置，为所有服务自动应用命名空间感知的实例发现逻辑。
 */
@Configuration(proxyBeanMethods = false)
public class AegisNamespaceLoadBalancerClientConfiguration {

    @Bean
    @ConditionalOnMissingBean(ServiceInstanceListSupplier.class)
    public ServiceInstanceListSupplier aegisServiceInstanceListSupplier(Environment environment,
                                                                       NacosDiscoveryProperties discoveryProperties,
                                                                       NacosNamingServiceRegistry namingServiceRegistry) {
        String serviceId = environment.getProperty(LoadBalancerClientFactory.PROPERTY_NAME);
        Assert.hasText(serviceId, "'serviceId' must not be empty");
        return new NamespaceAwareNacosServiceInstanceListSupplier(serviceId, discoveryProperties, namingServiceRegistry);
    }
}

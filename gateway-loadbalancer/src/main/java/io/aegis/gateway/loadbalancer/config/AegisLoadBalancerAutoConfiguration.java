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

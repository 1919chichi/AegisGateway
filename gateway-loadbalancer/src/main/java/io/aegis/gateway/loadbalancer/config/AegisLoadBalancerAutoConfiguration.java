package io.aegis.gateway.loadbalancer.config;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.cloud.nacos.loadbalancer.LoadBalancerNacosAutoConfiguration;
import com.alibaba.nacos.api.naming.NamingService;
import io.aegis.gateway.core.config.AegisCoreAutoConfiguration;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.loadbalancer.discovery.NacosNamingServiceRegistry;
import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicyRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.loadbalancer.annotation.LoadBalancerClients;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.ObjectMapper;

/**
 * gateway-loadbalancer 模块的自动配置入口。
 * <p>
 * 注册 {@link NacosNamingServiceRegistry} Bean，并通过 {@code @LoadBalancerClients} 将
 * {@link AegisNamespaceLoadBalancerClientConfiguration} 设为所有 LoadBalancer 客户端的
 * 默认配置，使命名空间感知的 Supplier 和一致性哈希策略生效。
 * <p>
 * 同时注册全局唯一的 {@link LoadBalancePolicyRepository}：按 serviceId 装配的每个
 * `ConsistentHashReactiveLoadBalancer` 子容器都从 parent 容器共享读取这一份 policy 快照，
 * 不需要每个 serviceId 各自监听一遍 Nacos governance 配置。该 Bean 独立以
 * {@code @ConditionalOnBean(NacosConfigSyncService.class)} 限定条件（而不是把整个自动配置类
 * 挂在这个条件上），避免影响本模块其他 Bean（如 {@code NacosNamingServiceRegistry}）在
 * 单独测试场景下的既有装配行为。
 * <p>
 * 在 {@link LoadBalancerNacosAutoConfiguration} 之后运行，确保 Nacos 相关 Bean 已就绪；
 * 在 {@link AegisCoreAutoConfiguration} 之后运行，确保 {@link NacosConfigSyncService}
 * 已经注册，{@code @ConditionalOnBean(NacosConfigSyncService.class)} 才能可靠生效。
 */
@AutoConfiguration(after = { LoadBalancerNacosAutoConfiguration.class, AegisCoreAutoConfiguration.class })
@ConditionalOnClass({ NamingService.class, ServiceInstanceListSupplier.class })
@ConditionalOnBean(NacosDiscoveryProperties.class)
@LoadBalancerClients(defaultConfiguration = AegisNamespaceLoadBalancerClientConfiguration.class)
public class AegisLoadBalancerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public NacosNamingServiceRegistry nacosNamingServiceRegistry(NacosDiscoveryProperties discoveryProperties) {
        return new NacosNamingServiceRegistry(discoveryProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(NacosConfigSyncService.class)
    public LoadBalancePolicyRepository loadBalancePolicyRepository(NacosConfigSyncService syncService,
                                                                    ObjectMapper objectMapper) {
        return new LoadBalancePolicyRepository(syncService, objectMapper);
    }
}

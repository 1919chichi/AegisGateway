package io.aegis.gateway.loadbalancer.config;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import io.aegis.gateway.loadbalancer.discovery.NamespaceAwareNacosServiceInstanceListSupplier;
import io.aegis.gateway.loadbalancer.discovery.NacosNamingServiceRegistry;
import io.aegis.gateway.loadbalancer.loadbalance.ConsistentHashReactiveLoadBalancer;
import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicyRepository;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.loadbalancer.core.ReactorServiceInstanceLoadBalancer;
import org.springframework.cloud.loadbalancer.core.RoundRobinLoadBalancer;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.cloud.loadbalancer.support.LoadBalancerClientFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;

/**
 * 每个 LoadBalancer 客户端的配置类，为对应 serviceId 创建
 * {@link NamespaceAwareNacosServiceInstanceListSupplier} 和 {@link ConsistentHashReactiveLoadBalancer}。
 * <p>
 * 由 {@link AegisLoadBalancerAutoConfiguration} 通过 {@code @LoadBalancerClients(defaultConfiguration)}
 * 注册为全局默认配置，为所有服务自动应用命名空间感知的实例发现逻辑和一致性哈希策略
 * （是否真正启用一致性哈希由 {@link LoadBalancePolicyRepository} 里该 serviceId 是否存在
 * policy 决定，未配置时 {@link ConsistentHashReactiveLoadBalancer} 内部委托给标准轮询，
 * 对外行为与此前完全一致）。
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

    // 隐式条件类型 = 方法返回类型 ReactorServiceInstanceLoadBalancer，与 SCG 自身默认 Bean
    // （LoadBalancerClientConfiguration#reactorServiceInstanceLoadBalancer，隐式类型
    // ReactorLoadBalancer）互斥；本配置类先于 SCG 默认配置类注册（见
    // NamedContextFactory#registerBeans 的 "default." 前缀分支），因此本 Bean 先注册，
    // SCG 默认 Bean 的 @ConditionalOnMissingBean 检测到已存在后自动回退。
    @Bean
    @ConditionalOnMissingBean
    public ReactorServiceInstanceLoadBalancer aegisConsistentHashLoadBalancer(
            Environment environment,
            LoadBalancerClientFactory clientFactory,
            ObjectProvider<LoadBalancePolicyRepository> policyRepositoryProvider) {
        String serviceId = environment.getProperty(LoadBalancerClientFactory.PROPERTY_NAME);
        Assert.hasText(serviceId, "'serviceId' must not be empty");
        ObjectProvider<ServiceInstanceListSupplier> supplierProvider =
                clientFactory.getLazyProvider(serviceId, ServiceInstanceListSupplier.class);
        LoadBalancePolicyRepository policyRepository = policyRepositoryProvider.getIfAvailable();
        if (policyRepository == null) {
            // LoadBalancePolicyRepository 声明为可缺失的 Bean（NacosConfigSyncService 不存在时不注册，
            // 见 AegisLoadBalancerAutoConfiguration），此处必须优雅降级为标准轮询——否则把一个
            // "必需依赖缺失"的场景变成 LB 子容器 NoSuchBeanDefinitionException 刷新失败，
            // 导致该 serviceId 的所有请求 500
            return new RoundRobinLoadBalancer(supplierProvider, serviceId);
        }
        return new ConsistentHashReactiveLoadBalancer(supplierProvider, serviceId, policyRepository);
    }
}

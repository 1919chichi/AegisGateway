package io.aegis.gateway.loadbalancer.config;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.cloud.nacos.NacosServiceManager;
import com.alibaba.cloud.nacos.util.InetIPv6Utils;
import io.aegis.gateway.loadbalancer.discovery.NacosNamingServiceRegistry;
import io.aegis.gateway.loadbalancer.loadbalance.ConsistentHashReactiveLoadBalancer;
import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicyRepository;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.loadbalancer.LoadBalancerClientsProperties;
import org.springframework.cloud.commons.util.InetUtils;
import org.springframework.cloud.loadbalancer.annotation.LoadBalancerClientSpecification;
import org.springframework.cloud.loadbalancer.core.ReactorServiceInstanceLoadBalancer;
import org.springframework.cloud.loadbalancer.support.LoadBalancerClientFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 走 {@link LoadBalancerClientFactory} 真实解析路径（而不是直接 {@code new} 再 {@code instanceof}）
 * 验证 {@link ConsistentHashReactiveLoadBalancer} 能被 Spring Cloud Gateway 实际取到。
 * <p>
 * SCG 的 {@code ReactiveLoadBalancerClientFilter} 通过
 * {@code clientFactory.getInstance(serviceId, ReactorServiceInstanceLoadBalancer.class)} 查找 Bean——
 * 这条测试复现了完整的 {@code NamedContextFactory} 子容器装配路径（默认配置类注册顺序、
 * {@code @ConditionalOnMissingBean} 互斥关系），只在这条路径上才能验证出"Bean 造出来了但框架不认"
 * 这类静默失败（见 final-review C1）。
 */
class ConsistentHashLoadBalancerResolutionTest {

    @Test
    void resolvesConsistentHashLoadBalancer_throughRealLoadBalancerClientFactory() {
        AnnotationConfigApplicationContext parent = new AnnotationConfigApplicationContext();
        parent.registerBean(InetIPv6Utils.class, () -> mock(InetIPv6Utils.class));
        InetUtils inetUtils = mock(InetUtils.class);
        InetUtils.HostInfo hostInfo = mock(InetUtils.HostInfo.class);
        when(hostInfo.getIpAddress()).thenReturn("127.0.0.1");
        when(inetUtils.findFirstNonLoopbackHostInfo()).thenReturn(hostInfo);
        parent.registerBean(InetUtils.class, () -> inetUtils);
        parent.registerBean(NacosServiceManager.class, () -> mock(NacosServiceManager.class));
        parent.registerBean(NacosDiscoveryProperties.class, NacosDiscoveryProperties::new);
        parent.registerBean(NacosNamingServiceRegistry.class, () -> mock(NacosNamingServiceRegistry.class));
        LoadBalancePolicyRepository policyRepository = mock(LoadBalancePolicyRepository.class);
        when(policyRepository.findByServiceId("order-service")).thenReturn(Optional.empty());
        parent.registerBean(LoadBalancePolicyRepository.class, () -> policyRepository);

        LoadBalancerClientsProperties properties = new LoadBalancerClientsProperties();
        LoadBalancerClientFactory clientFactory = new LoadBalancerClientFactory(properties);
        parent.registerBean(LoadBalancerClientFactory.class, () -> clientFactory);
        parent.refresh();

        clientFactory.setApplicationContext(parent);
        clientFactory.setConfigurations(List.of(new LoadBalancerClientSpecification(
                "default." + AegisNamespaceLoadBalancerClientConfiguration.class.getName(),
                new Class<?>[] { AegisNamespaceLoadBalancerClientConfiguration.class })));

        ReactorServiceInstanceLoadBalancer resolved =
                clientFactory.getInstance("order-service", ReactorServiceInstanceLoadBalancer.class);

        assertThat(resolved).isInstanceOf(ConsistentHashReactiveLoadBalancer.class);
    }
}

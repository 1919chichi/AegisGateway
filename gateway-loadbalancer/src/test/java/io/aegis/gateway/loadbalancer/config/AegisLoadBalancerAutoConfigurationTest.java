package io.aegis.gateway.loadbalancer.config;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.cloud.nacos.NacosServiceManager;
import com.alibaba.cloud.nacos.util.InetIPv6Utils;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.loadbalancer.discovery.NamespaceAwareNacosServiceInstanceListSupplier;
import io.aegis.gateway.loadbalancer.discovery.NacosNamingServiceRegistry;
import io.aegis.gateway.loadbalancer.loadbalance.ConsistentHashReactiveLoadBalancer;
import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicyRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.commons.util.InetUtils;
import org.springframework.cloud.loadbalancer.core.ReactorServiceInstanceLoadBalancer;
import org.springframework.cloud.loadbalancer.core.RoundRobinLoadBalancer;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.cloud.loadbalancer.support.LoadBalancerClientFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.MapPropertySource;
import org.springframework.mock.env.MockEnvironment;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AegisLoadBalancerAutoConfigurationTest {

    @Configuration(proxyBeanMethods = false)
    static class NacosDependenciesConfig {
        @Bean
        InetIPv6Utils inetIPv6Utils() {
            return mock(InetIPv6Utils.class);
        }

        @Bean
        InetUtils inetUtils() {
            InetUtils mock = mock(InetUtils.class);
            InetUtils.HostInfo hostInfo = mock(InetUtils.HostInfo.class);
            when(hostInfo.getIpAddress()).thenReturn("127.0.0.1");
            when(mock.findFirstNonLoopbackHostInfo()).thenReturn(hostInfo);
            return mock;
        }

        @Bean
        NacosServiceManager nacosServiceManager() {
            return mock(NacosServiceManager.class);
        }

        @Bean
        NacosDiscoveryProperties nacosDiscoveryProperties() {
            return new NacosDiscoveryProperties();
        }
    }

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(AegisLoadBalancerAutoConfiguration.class))
            .withPropertyValues("spring.cloud.nacos.discovery.enabled=false")
            .withUserConfiguration(NacosDependenciesConfig.class);

    @Test
    void autoConfiguration_shouldRegisterNamingServiceRegistry() {
        contextRunner.run(context ->
                assertThat(context).hasSingleBean(NacosNamingServiceRegistry.class));
    }

    @Test
    void clientConfiguration_shouldCreateNamespaceAwareSupplierForServiceContext() {
        MockEnvironment environment = new MockEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("loadbalancer-client", Map.of(
                LoadBalancerClientFactory.PROPERTY_NAME, "user-service")));
        NacosDiscoveryProperties discoveryProperties = new NacosDiscoveryProperties();
        NacosNamingServiceRegistry registry = mock(NacosNamingServiceRegistry.class);
        AegisNamespaceLoadBalancerClientConfiguration configuration =
                new AegisNamespaceLoadBalancerClientConfiguration();

        ServiceInstanceListSupplier supplier = configuration.aegisServiceInstanceListSupplier(
                environment, discoveryProperties, registry);

        assertThat(supplier).isInstanceOf(NamespaceAwareNacosServiceInstanceListSupplier.class);
        assertThat(supplier.getServiceId()).isEqualTo("user-service");
    }

    @Test
    void autoConfiguration_shouldRegisterLoadBalancePolicyRepository_whenNacosConfigSyncServiceBeanPresent() {
        contextRunner
                .withBean(NacosConfigSyncService.class, () -> mock(NacosConfigSyncService.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .run(context -> assertThat(context).hasSingleBean(LoadBalancePolicyRepository.class));
    }

    @Test
    void autoConfiguration_shouldNotRegisterLoadBalancePolicyRepository_whenNacosConfigSyncServiceBeanMissing() {
        contextRunner
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .run(context -> assertThat(context).doesNotHaveBean(LoadBalancePolicyRepository.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void clientConfiguration_shouldCreateConsistentHashLoadBalancerBean() {
        MockEnvironment environment = new MockEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("loadbalancer-client", Map.of(
                LoadBalancerClientFactory.PROPERTY_NAME, "order-service")));
        LoadBalancerClientFactory clientFactory = mock(LoadBalancerClientFactory.class);
        ObjectProvider<ServiceInstanceListSupplier> supplierProvider = mock(ObjectProvider.class);
        when(clientFactory.getLazyProvider("order-service", ServiceInstanceListSupplier.class))
                .thenReturn(supplierProvider);
        LoadBalancePolicyRepository policyRepository = mock(LoadBalancePolicyRepository.class);
        ObjectProvider<LoadBalancePolicyRepository> policyRepositoryProvider = mock(ObjectProvider.class);
        when(policyRepositoryProvider.getIfAvailable()).thenReturn(policyRepository);
        AegisNamespaceLoadBalancerClientConfiguration configuration =
                new AegisNamespaceLoadBalancerClientConfiguration();

        ReactorServiceInstanceLoadBalancer loadBalancer = configuration.aegisConsistentHashLoadBalancer(
                environment, clientFactory, policyRepositoryProvider);

        assertThat(loadBalancer).isInstanceOf(ConsistentHashReactiveLoadBalancer.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void clientConfiguration_shouldFallBackToPlainRoundRobin_whenPolicyRepositoryProviderUnavailable() {
        MockEnvironment environment = new MockEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("loadbalancer-client", Map.of(
                LoadBalancerClientFactory.PROPERTY_NAME, "order-service")));
        LoadBalancerClientFactory clientFactory = mock(LoadBalancerClientFactory.class);
        ObjectProvider<ServiceInstanceListSupplier> supplierProvider = mock(ObjectProvider.class);
        when(clientFactory.getLazyProvider("order-service", ServiceInstanceListSupplier.class))
                .thenReturn(supplierProvider);
        // LoadBalancePolicyRepository 声明为可缺失的 Bean（NacosConfigSyncService 不存在时不注册），
        // 消费方必须能优雅降级，而不是让 LB 子容器因 NoSuchBeanDefinitionException 刷新失败
        ObjectProvider<LoadBalancePolicyRepository> policyRepositoryProvider = mock(ObjectProvider.class);
        when(policyRepositoryProvider.getIfAvailable()).thenReturn(null);
        AegisNamespaceLoadBalancerClientConfiguration configuration =
                new AegisNamespaceLoadBalancerClientConfiguration();

        ReactorServiceInstanceLoadBalancer loadBalancer = configuration.aegisConsistentHashLoadBalancer(
                environment, clientFactory, policyRepositoryProvider);

        assertThat(loadBalancer).isInstanceOf(RoundRobinLoadBalancer.class);
        assertThat(loadBalancer).isNotInstanceOf(ConsistentHashReactiveLoadBalancer.class);
    }
}

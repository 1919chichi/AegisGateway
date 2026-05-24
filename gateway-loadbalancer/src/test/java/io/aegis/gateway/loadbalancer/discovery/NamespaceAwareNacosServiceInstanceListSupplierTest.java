package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import io.aegis.gateway.core.model.AegisDiscoveryMetadata;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.DefaultRequest;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestData;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR;

class NamespaceAwareNacosServiceInstanceListSupplierTest {

    @Test
    void get_shouldQueryNamespaceFromSelectedRouteMetadata() throws Exception {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        NacosNamingServiceRegistry registry = mock(NacosNamingServiceRegistry.class);
        NamingService namingService = mock(NamingService.class);
        when(registry.getNamingService(new AegisDiscoveryMetadata("gray", "GROUP_A"))).thenReturn(namingService);
        when(namingService.selectInstances("user-service", "GROUP_A", true)).thenReturn(List.of(instance()));
        NamespaceAwareNacosServiceInstanceListSupplier supplier =
                new NamespaceAwareNacosServiceInstanceListSupplier("user-service", defaults, registry);

        StepVerifier.create(supplier.get(request(route("user-service-gray", "gray", "GROUP_A"))).next())
                .assertNext(instances -> {
                    assertThat(instances).hasSize(1);
                    ServiceInstance serviceInstance = instances.getFirst();
                    assertThat(serviceInstance.getServiceId()).isEqualTo("user-service");
                    assertThat(serviceInstance.getHost()).isEqualTo("10.0.0.7");
                    assertThat(serviceInstance.getPort()).isEqualTo(8080);
                    assertThat(serviceInstance.getMetadata())
                            .containsEntry("aegis.nacos.namespace", "gray")
                            .containsEntry("aegis.nacos.group", "GROUP_A")
                            .containsEntry("version", "v2");
                })
                .verifyComplete();

        verify(namingService).selectInstances("user-service", "GROUP_A", true);
    }

    @Test
    void get_shouldFallbackToDefaultDiscoveryWhenRouteMetadataIsMissing() throws Exception {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        NacosNamingServiceRegistry registry = mock(NacosNamingServiceRegistry.class);
        NamingService namingService = mock(NamingService.class);
        when(registry.getNamingService(new AegisDiscoveryMetadata("public", "DEFAULT_GROUP"))).thenReturn(namingService);
        when(namingService.selectInstances("user-service", "DEFAULT_GROUP", true)).thenReturn(List.of(instance()));
        NamespaceAwareNacosServiceInstanceListSupplier supplier =
                new NamespaceAwareNacosServiceInstanceListSupplier("user-service", defaults, registry);

        StepVerifier.create(supplier.get(request(routeWithoutMetadata())).next())
                .assertNext(instances -> assertThat(instances).hasSize(1))
                .verifyComplete();

        verify(namingService).selectInstances("user-service", "DEFAULT_GROUP", true);
    }

    @Test
    void get_shouldReturnEmptyListWhenNacosQueryFails() throws Exception {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        NacosNamingServiceRegistry registry = mock(NacosNamingServiceRegistry.class);
        NamingService namingService = mock(NamingService.class);
        when(registry.getNamingService(new AegisDiscoveryMetadata("gray", "GROUP_A"))).thenReturn(namingService);
        when(namingService.selectInstances("user-service", "GROUP_A", true)).thenThrow(new RuntimeException("nacos down"));
        NamespaceAwareNacosServiceInstanceListSupplier supplier =
                new NamespaceAwareNacosServiceInstanceListSupplier("user-service", defaults, registry);

        StepVerifier.create(supplier.get(request(route("user-service-gray", "gray", "GROUP_A"))).next())
                .assertNext(instances -> assertThat(instances).isEmpty())
                .verifyComplete();
    }

    private static NacosDiscoveryProperties defaults(String namespace, String group) {
        NacosDiscoveryProperties properties = new NacosDiscoveryProperties();
        properties.setNamespace(namespace);
        properties.setGroup(group);
        return properties;
    }

    private static Request<RequestDataContext> request(Route route) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/users/1").build());
        exchange.getAttributes().put(GATEWAY_ROUTE_ATTR, route);
        RequestData requestData = new RequestData(exchange.getRequest(), exchange.getAttributes());
        return new DefaultRequest<>(new RequestDataContext(requestData, "default"));
    }

    private static Route route(String routeId, String namespace, String group) {
        return Route.async()
                .id(routeId)
                .uri("lb://user-service")
                .predicate(exchange -> true)
                .metadata(Map.of("discovery", Map.of(
                        "namespace", namespace,
                        "group", group)))
                .build();
    }

    private static Route routeWithoutMetadata() {
        return Route.async()
                .id("user-service-default")
                .uri("lb://user-service")
                .predicate(exchange -> true)
                .build();
    }

    private static Instance instance() {
        Instance instance = new Instance();
        instance.setInstanceId("instance-1");
        instance.setIp("10.0.0.7");
        instance.setPort(8080);
        instance.setHealthy(true);
        instance.setEnabled(true);
        instance.setWeight(10.0);
        instance.setClusterName("HZ");
        instance.setMetadata(Map.of("version", "v2"));
        return instance;
    }

    @Test
    void get_shouldUseExchangeAttributeOverRouteMetadata() throws Exception {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        NacosNamingServiceRegistry registry = mock(NacosNamingServiceRegistry.class);
        NamingService namingService = mock(NamingService.class);
        when(registry.getNamingService(new AegisDiscoveryMetadata("prod-canary", "DEFAULT_GROUP"))).thenReturn(namingService);
        when(namingService.selectInstances("user-service", "DEFAULT_GROUP", true)).thenReturn(List.of(instance()));
        NamespaceAwareNacosServiceInstanceListSupplier supplier =
                new NamespaceAwareNacosServiceInstanceListSupplier("user-service", defaults, registry);

        // route metadata 指向 "gray"，exchange attribute 指向 "prod-canary"，attribute 应优先
        StepVerifier.create(supplier.get(requestWithGrayAttribute(
                        route("user-service-gray", "gray", "GROUP_A"),
                        new AegisDiscoveryMetadata("prod-canary", "DEFAULT_GROUP"))).next())
                .assertNext(instances -> assertThat(instances).hasSize(1))
                .verifyComplete();

        verify(namingService).selectInstances("user-service", "DEFAULT_GROUP", true);
    }

    private static Request<RequestDataContext> requestWithGrayAttribute(Route route, AegisDiscoveryMetadata grayMetadata) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/users/1").build());
        exchange.getAttributes().put(GATEWAY_ROUTE_ATTR, route);
        exchange.getAttributes().put(AegisDiscoveryMetadata.ATTR_KEY, grayMetadata);
        RequestData requestData = new RequestData(exchange.getRequest(), exchange.getAttributes());
        return new DefaultRequest<>(new RequestDataContext(requestData, "default"));
    }
}

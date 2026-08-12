package io.aegis.gateway.loadbalancer.loadbalance;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.aegis.gateway.loadbalancer.hash.DefaultHashKeyExtractor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.DefaultRequest;
import org.springframework.cloud.client.loadbalancer.DefaultResponse;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestData;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.loadbalancer.core.RoundRobinLoadBalancer;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ConsistentHashReactiveLoadBalancerTest {

    private static final String SERVICE_ID = "order-service";

    @SuppressWarnings("unchecked")
    private final ObjectProvider<ServiceInstanceListSupplier> supplierProvider = mock(ObjectProvider.class);
    private final LoadBalancePolicyRepository policyRepository = mock(LoadBalancePolicyRepository.class);
    private final RoundRobinLoadBalancer delegate = mock(RoundRobinLoadBalancer.class);

    @Test
    void choose_shouldDelegateToRoundRobin_whenNoPolicyConfigured() {
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.empty());
        ServiceInstance instance = instance("10.0.0.1", 8080, null);
        when(delegate.choose(any())).thenReturn(Mono.just(new DefaultResponse(instance)));
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, new HashKeyMissingLogger());
        Request<RequestDataContext> request = requestWithHeader("X-User-Id", "u1");

        StepVerifier.create(loadBalancer.choose(request))
                .assertNext(response -> assertThat(response.getServer()).isEqualTo(instance))
                .verifyComplete();

        verify(delegate).choose(request);
        verifyNoInteractions(supplierProvider);
    }

    @Test
    void choose_shouldRouteToSameInstance_forSameKey_whenPolicyConfigured() {
        LoadBalancePolicy policy = new LoadBalancePolicy(
                SERVICE_ID, LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", 160);
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.of(policy));
        List<ServiceInstance> instances = List.of(
                instance("10.0.0.1", 8080, null),
                instance("10.0.0.2", 8080, null),
                instance("10.0.0.3", 8080, null));
        ServiceInstanceListSupplier supplier = mock(ServiceInstanceListSupplier.class);
        when(supplier.get(any())).thenReturn(Flux.just(instances));
        when(supplierProvider.getIfAvailable()).thenReturn(supplier);
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, new HashKeyMissingLogger());
        Request<RequestDataContext> request = requestWithHeader("X-User-Id", "u10086");

        ServiceInstance first = choose(loadBalancer, request);
        ServiceInstance second = choose(loadBalancer, request);

        assertThat(first).isEqualTo(second);
        assertThat(instances).contains(first);
        verifyNoInteractions(delegate);
    }

    @Test
    void choose_shouldDegradeToRoundRobin_whenKeyMissing() {
        LoadBalancePolicy policy = new LoadBalancePolicy(
                SERVICE_ID, LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", 160);
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.of(policy));
        ServiceInstanceListSupplier supplier = mock(ServiceInstanceListSupplier.class);
        when(supplier.get(any())).thenReturn(Flux.just(List.of(instance("10.0.0.1", 8080, null))));
        when(supplierProvider.getIfAvailable()).thenReturn(supplier);
        ServiceInstance fallback = instance("10.0.0.9", 8080, null);
        when(delegate.choose(any())).thenReturn(Mono.just(new DefaultResponse(fallback)));
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, new HashKeyMissingLogger());
        // 请求没有携带 policy 要求的 X-User-Id header
        Request<RequestDataContext> request = requestWithHeader("X-Other-Header", "irrelevant");

        StepVerifier.create(loadBalancer.choose(request))
                .assertNext(response -> assertThat(response.getServer()).isEqualTo(fallback))
                .verifyComplete();

        verify(delegate).choose(request);
    }

    @Test
    void choose_shouldOnlyCallMissingLoggerOncePerCall_andDelegateThrottlingToIt() {
        LoadBalancePolicy policy = new LoadBalancePolicy(
                SERVICE_ID, LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", 160);
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.of(policy));
        ServiceInstanceListSupplier supplier = mock(ServiceInstanceListSupplier.class);
        when(supplier.get(any())).thenReturn(Flux.just(List.of(instance("10.0.0.1", 8080, null))));
        when(supplierProvider.getIfAvailable()).thenReturn(supplier);
        when(delegate.choose(any())).thenReturn(Mono.just(new DefaultResponse(instance("10.0.0.9", 8080, null))));
        HashKeyMissingLogger missingLogger = mock(HashKeyMissingLogger.class);
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, missingLogger);
        Request<RequestDataContext> request = requestWithHeader("X-Other-Header", "irrelevant");

        loadBalancer.choose(request).block();
        loadBalancer.choose(request).block();

        // 节流窗口本身的语义在 HashKeyMissingLoggerTest 里验证；这里只验证每次 key 缺失
        // 都正确调用了 warnIfDue，把"是否真的打印"这个决定完全交给该协作者
        verify(missingLogger, times(2)).warnIfDue(SERVICE_ID, policy);
    }

    @Test
    void choose_shouldReturnEmptyResponse_whenCandidateInstanceListIsEmpty() {
        LoadBalancePolicy policy = new LoadBalancePolicy(
                SERVICE_ID, LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", 160);
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.of(policy));
        ServiceInstanceListSupplier supplier = mock(ServiceInstanceListSupplier.class);
        when(supplier.get(any())).thenReturn(Flux.just(List.of()));
        when(supplierProvider.getIfAvailable()).thenReturn(supplier);
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, new HashKeyMissingLogger());
        Request<RequestDataContext> request = requestWithHeader("X-User-Id", "u1");

        StepVerifier.create(loadBalancer.choose(request))
                .assertNext(response -> assertThat(response.hasServer()).isFalse())
                .verifyComplete();

        verifyNoInteractions(delegate);
    }

    @Test
    void choose_shouldFallBackToRoundRobin_whenSupplierUnavailable() {
        LoadBalancePolicy policy = new LoadBalancePolicy(
                SERVICE_ID, LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", 160);
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.of(policy));
        when(supplierProvider.getIfAvailable()).thenReturn(null);
        ServiceInstance fallback = instance("10.0.0.9", 8080, null);
        when(delegate.choose(any())).thenReturn(Mono.just(new DefaultResponse(fallback)));
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, new HashKeyMissingLogger());
        Request<RequestDataContext> request = requestWithHeader("X-User-Id", "u1");

        StepVerifier.create(loadBalancer.choose(request))
                .assertNext(response -> assertThat(response.getServer()).isEqualTo(fallback))
                .verifyComplete();
    }

    @Test
    void choose_shouldOnlyRerouteKeysThatHitRemovedInstance_whenCandidateListShrinks() {
        LoadBalancePolicy policy = new LoadBalancePolicy(
                SERVICE_ID, LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", 160);
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.of(policy));
        List<ServiceInstance> fiveInstances = List.of(
                instance("10.0.0.1", 8080, null), instance("10.0.0.2", 8080, null),
                instance("10.0.0.3", 8080, null), instance("10.0.0.4", 8080, null),
                instance("10.0.0.5", 8080, null));
        ServiceInstance removed = fiveInstances.get(2);
        List<ServiceInstance> fourInstances = fiveInstances.stream().filter(i -> !i.equals(removed)).toList();
        ServiceInstanceListSupplier supplier = mock(ServiceInstanceListSupplier.class);
        when(supplierProvider.getIfAvailable()).thenReturn(supplier);
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, new HashKeyMissingLogger());

        when(supplier.get(any())).thenReturn(Flux.just(fiveInstances));
        for (int i = 0; i < 200; i++) {
            Request<RequestDataContext> request = requestWithHeader("X-User-Id", "user-" + i);
            ServiceInstance before = choose(loadBalancer, request);

            when(supplier.get(any())).thenReturn(Flux.just(fourInstances));
            ServiceInstance after = choose(loadBalancer, request);
            when(supplier.get(any())).thenReturn(Flux.just(fiveInstances));

            if (!before.equals(after)) {
                assertThat(before).isEqualTo(removed);
                assertThat(after).isNotEqualTo(removed);
            }
        }
    }

    private ConsistentHashReactiveLoadBalancer newLoadBalancer(RoundRobinLoadBalancer delegate,
                                                                HashKeyMissingLogger missingLogger) {
        return new ConsistentHashReactiveLoadBalancer(supplierProvider, SERVICE_ID, policyRepository,
                delegate, new DefaultHashKeyExtractor(), missingLogger,
                Caffeine.newBuilder().maximumSize(2).build());
    }

    private static ServiceInstance choose(ConsistentHashReactiveLoadBalancer loadBalancer, Request<?> request) {
        Response<ServiceInstance> response = loadBalancer.choose(request).block();
        return response.getServer();
    }

    private static ServiceInstance instance(String host, int port, String weightMetadata) {
        Map<String, String> metadata = weightMetadata == null ? Map.of() : Map.of("nacos.weight", weightMetadata);
        return new DefaultServiceInstance("instance-" + host, SERVICE_ID, host, port, false, metadata);
    }

    private static Request<RequestDataContext> requestWithHeader(String name, String value) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders").header(name, value).build());
        RequestData requestData = new RequestData(exchange.getRequest(), exchange.getAttributes());
        return new DefaultRequest<>(new RequestDataContext(requestData, "default"));
    }
}

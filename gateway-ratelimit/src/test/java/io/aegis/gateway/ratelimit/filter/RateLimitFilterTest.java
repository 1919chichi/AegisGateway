package io.aegis.gateway.ratelimit.filter;

import io.aegis.gateway.core.filter.AegisFilterOrder;
import io.aegis.gateway.core.model.AegisErrorCode;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.ratelimit.core.ReactiveRateLimiterGateway;
import io.aegis.gateway.ratelimit.core.RedissonClientManager;
import io.aegis.gateway.ratelimit.model.RateLimitPolicy;
import io.aegis.gateway.ratelimit.model.RateLimitRule;
import io.aegis.gateway.ratelimit.model.RateLimitType;
import io.aegis.gateway.ratelimit.repository.RateLimitPolicyRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR;

class RateLimitFilterTest {

    private final RateLimitPolicyRepository repository = mock(RateLimitPolicyRepository.class);
    private final ReactiveRateLimiterGateway limiterGateway = mock(ReactiveRateLimiterGateway.class);
    private final RateLimitFilter filter = new RateLimitFilter(repository, limiterGateway, new ObjectMapper());

    @Test
    void shouldPassThroughWhenRouteHasNoRateLimitPolicy() {
        var exchange = exchangeWithRoute(Map.of());
        GatewayFilterChain chain = chain(exchange);

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        verify(chain).filter(exchange);
        verify(limiterGateway, never()).tryAcquire(any(), any());
    }

    @Test
    void shouldPassThroughWhenPolicyIdDoesNotExist() {
        var exchange = exchangeWithRoute(Map.of("rateLimit", Map.of("policyId", "missing")));
        GatewayFilterChain chain = chain(exchange);
        when(repository.findById("missing")).thenReturn(Optional.empty());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        verify(chain).filter(exchange);
        verify(limiterGateway, never()).tryAcquire(any(), any());
    }

    @Test
    void shouldPassThroughWhenAllMatchedRulesAcquireTokens() {
        RateLimitPolicy policy = policy(List.of(
                new RateLimitRule("service-total", RateLimitType.SERVICE, 1000, 500, null, null),
                new RateLimitRule("login-path", RateLimitType.PATH, 50, 10, "/api/users/login", null)
        ));
        var exchange = exchangeWithRoute(Map.of("rateLimit", Map.of("policyId", "user-policy")));
        GatewayFilterChain chain = chain(exchange);
        when(repository.findById("user-policy")).thenReturn(Optional.of(policy));
        when(limiterGateway.tryAcquire(any(), any())).thenReturn(Mono.just(true));

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        verify(chain).filter(exchange);
        verify(limiterGateway).tryAcquire("aegis:ratelimit:user-policy:login-path", policy.rules().get(1));
        verify(limiterGateway).tryAcquire("aegis:ratelimit:user-policy:service-total:user-service", policy.rules().get(0));
    }

    @Test
    void shouldWriteRuleSpecific429WhenFirstMatchedRuleFails() throws Exception {
        RateLimitPolicy policy = policy(List.of(
                new RateLimitRule("per-user", RateLimitType.USER, 60, 10, null, "X-User-Id"),
                new RateLimitRule("service-total", RateLimitType.SERVICE, 1000, 500, null, null)
        ));
        var exchange = exchangeWithRoute(Map.of("rateLimit", Map.of("policyId", "user-policy")));
        GatewayFilterChain chain = chain(exchange);
        when(repository.findById("user-policy")).thenReturn(Optional.of(policy));
        when(limiterGateway.tryAcquire(eq("aegis:ratelimit:user-policy:per-user:10001"), any()))
                .thenReturn(Mono.just(false));

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        String body = exchange.getResponse().getBodyAsString().block();
        assertThat(body).contains("\"code\":%d".formatted(AegisErrorCode.RATE_LIMIT_USER.getCode()));
        verify(chain, never()).filter(exchange);
    }

    @Test
    void shouldWriteServiceErrorCodeWhenServiceRuleFails() {
        RateLimitPolicy policy = policy(List.of(
                new RateLimitRule("service-total", RateLimitType.SERVICE, 1000, 500, null, null)
        ));
        var exchange = exchangeWithRoute(Map.of("rateLimit", Map.of("policyId", "user-policy")));
        GatewayFilterChain chain = chain(exchange);
        when(repository.findById("user-policy")).thenReturn(Optional.of(policy));
        when(limiterGateway.tryAcquire(any(), any())).thenReturn(Mono.just(false));

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(exchange.getResponse().getBodyAsString().block())
                .contains("\"code\":%d".formatted(AegisErrorCode.RATE_LIMIT_SERVICE.getCode()));
        verify(chain, never()).filter(exchange);
    }

    @Test
    void shouldWritePathErrorCodeWhenPathRuleFails() {
        RateLimitPolicy policy = policy(List.of(
                new RateLimitRule("login-path", RateLimitType.PATH, 50, 10, "/api/users/login", null)
        ));
        var exchange = exchangeWithRoute(Map.of("rateLimit", Map.of("policyId", "user-policy")));
        GatewayFilterChain chain = chain(exchange);
        when(repository.findById("user-policy")).thenReturn(Optional.of(policy));
        when(limiterGateway.tryAcquire(any(), any())).thenReturn(Mono.just(false));

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(exchange.getResponse().getBodyAsString().block())
                .contains("\"code\":%d".formatted(AegisErrorCode.RATE_LIMIT_PATH.getCode()));
        verify(chain, never()).filter(exchange);
    }

    @Test
    void shouldFailOpenWhenLimiterGatewayHasNoDecision() {
        // 空 Mono = Redis 未配置或重连中，按 fail-open 放行
        RateLimitPolicy policy = policy(List.of(
                new RateLimitRule("service-total", RateLimitType.SERVICE, 1000, 500, null, null)
        ));
        var exchange = exchangeWithRoute(Map.of("rateLimit", Map.of("policyId", "user-policy")));
        GatewayFilterChain chain = chain(exchange);
        when(repository.findById("user-policy")).thenReturn(Optional.of(policy));
        when(limiterGateway.tryAcquire(any(), any())).thenReturn(Mono.empty());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        verify(chain).filter(exchange);
    }

    @Test
    void shouldFailOpenWhenLimiterGatewayErrors() {
        RateLimitPolicy policy = policy(List.of(
                new RateLimitRule("service-total", RateLimitType.SERVICE, 1000, 500, null, null)
        ));
        var exchange = exchangeWithRoute(Map.of("rateLimit", Map.of("policyId", "user-policy")));
        GatewayFilterChain chain = chain(exchange);
        when(repository.findById("user-policy")).thenReturn(Optional.of(policy));
        when(limiterGateway.tryAcquire(any(), any())).thenReturn(Mono.error(new IllegalStateException("redis down")));

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        verify(chain).filter(exchange);
    }

    @Test
    void shouldNotTreatDownstreamChainErrorsAsRateLimitFailOpen() {
        RateLimitPolicy policy = policy(List.of(
                new RateLimitRule("service-total", RateLimitType.SERVICE, 1000, 500, null, null)
        ));
        var exchange = exchangeWithRoute(Map.of("rateLimit", Map.of("policyId", "user-policy")));
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        RuntimeException downstream = new RuntimeException("downstream failed");
        when(chain.filter(exchange)).thenReturn(Mono.error(downstream));
        when(repository.findById("user-policy")).thenReturn(Optional.of(policy));
        when(limiterGateway.tryAcquire(any(), any())).thenReturn(Mono.just(true));

        StepVerifier.create(filter.filter(exchange, chain))
                .expectErrorMatches(ex -> ex == downstream)
                .verify();

        verify(chain).filter(exchange);
    }

    @Test
    void getOrderShouldReturnRateLimitOrder() {
        assertThat(filter.getOrder()).isEqualTo(AegisFilterOrder.RATE_LIMIT);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldUseUpdatedRuleParametersAfterGovernanceHotReload() {
        // 端到端验证热更新：governance 推送新 capacity/refillRate 后，新请求把新参数传给 limiter gateway
        NacosConfigSyncService syncService = mock(NacosConfigSyncService.class);
        RedissonClientManager clientManager = mock(RedissonClientManager.class);
        RateLimitPolicyRepository realRepository =
                new RateLimitPolicyRepository(syncService, new ObjectMapper(), clientManager);
        ArgumentCaptor<Consumer<String>> listenerCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(syncService).registerGovernanceListener(listenerCaptor.capture());
        RateLimitFilter hotReloadFilter = new RateLimitFilter(realRepository, limiterGateway, new ObjectMapper());
        when(limiterGateway.tryAcquire(any(), any())).thenReturn(Mono.just(true));

        listenerCaptor.getValue().accept("""
                {"rateLimitPolicies":[{"id":"user-policy","rules":[{"id":"service-total","type":"SERVICE","capacity":10,"refillRate":5}]}]}
                """);
        var firstExchange = exchangeWithRoute(Map.of("rateLimit", Map.of("policyId", "user-policy")));
        StepVerifier.create(hotReloadFilter.filter(firstExchange, chain(firstExchange))).verifyComplete();

        listenerCaptor.getValue().accept("""
                {"rateLimitPolicies":[{"id":"user-policy","rules":[{"id":"service-total","type":"SERVICE","capacity":99,"refillRate":33}]}]}
                """);
        var secondExchange = exchangeWithRoute(Map.of("rateLimit", Map.of("policyId", "user-policy")));
        StepVerifier.create(hotReloadFilter.filter(secondExchange, chain(secondExchange))).verifyComplete();

        ArgumentCaptor<RateLimitRule> ruleCaptor = ArgumentCaptor.forClass(RateLimitRule.class);
        verify(limiterGateway, org.mockito.Mockito.times(2)).tryAcquire(any(), ruleCaptor.capture());
        assertThat(ruleCaptor.getAllValues().get(0).capacity()).isEqualTo(10);
        assertThat(ruleCaptor.getAllValues().get(0).refillRate()).isEqualTo(5);
        assertThat(ruleCaptor.getAllValues().get(1).capacity()).isEqualTo(99);
        assertThat(ruleCaptor.getAllValues().get(1).refillRate()).isEqualTo(33);
    }

    private static RateLimitPolicy policy(List<RateLimitRule> rules) {
        return new RateLimitPolicy("user-policy", rules);
    }

    private static MockServerWebExchange exchangeWithRoute(Map<String, Object> metadata) {
        var exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/users/login").header("X-User-Id", "10001").build());
        exchange.getAttributes().put(GATEWAY_ROUTE_ATTR, Route.async()
                .id("user-route")
                .uri("lb://user-service")
                .metadata(metadata)
                .predicate(e -> true)
                .build());
        return exchange;
    }

    private static GatewayFilterChain chain(MockServerWebExchange exchange) {
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());
        return chain;
    }
}

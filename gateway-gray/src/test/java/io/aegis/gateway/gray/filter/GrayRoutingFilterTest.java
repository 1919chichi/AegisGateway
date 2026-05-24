package io.aegis.gateway.gray.filter;

import io.aegis.gateway.core.filter.AegisFilterOrder;
import io.aegis.gateway.core.model.AegisDiscoveryMetadata;
import io.aegis.gateway.core.model.config.AegisRoute;
import io.aegis.gateway.core.model.config.AegisRoutesConfig;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GrayRoutingFilterTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private NacosConfigSyncService syncService;

    @BeforeEach
    void setUp() {
        syncService = mock(NacosConfigSyncService.class);
    }

    @Test
    void filter_shouldSetExchangeAttributeWhenRuleMatches() {
        when(syncService.getGovernanceConfigJson()).thenReturn("""
                {"gray":{"rules":[{"type":"header","key":"X-User-Type","value":"beta","targetRouteId":"user-canary"}]}}
                """);
        when(syncService.getRoutesConfig()).thenReturn(routesConfig("user-canary", "prod-canary", "DEFAULT_GROUP"));
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);
        ServerWebExchange exchange = exchange("X-User-Type", "beta");
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.<AegisDiscoveryMetadata>getAttribute(AegisDiscoveryMetadata.ATTR_KEY))
                .isEqualTo(new AegisDiscoveryMetadata("prod-canary", "DEFAULT_GROUP"));
        verify(chain).filter(exchange);
    }

    @Test
    void filter_shouldPassThroughWhenHeaderValueDoesNotMatch() {
        when(syncService.getGovernanceConfigJson()).thenReturn("""
                {"gray":{"rules":[{"type":"header","key":"X-User-Type","value":"beta","targetRouteId":"user-canary"}]}}
                """);
        when(syncService.getRoutesConfig()).thenReturn(routesConfig("user-canary", "prod-canary", "DEFAULT_GROUP"));
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);
        ServerWebExchange exchange = exchange("X-User-Type", "stable");
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.getAttributes()).doesNotContainKey(AegisDiscoveryMetadata.ATTR_KEY);
        verify(chain).filter(exchange);
    }

    @Test
    void filter_shouldPassThroughWhenNoRulesConfigured() {
        when(syncService.getGovernanceConfigJson()).thenReturn("{}");
        when(syncService.getRoutesConfig()).thenReturn(AegisRoutesConfig.empty());
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);
        ServerWebExchange exchange = exchange("X-User-Type", "beta");
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.getAttributes()).doesNotContainKey(AegisDiscoveryMetadata.ATTR_KEY);
    }

    @Test
    void filter_shouldPassThroughWhenTargetRouteIdNotFoundInRouteMap() {
        when(syncService.getGovernanceConfigJson()).thenReturn("""
                {"gray":{"rules":[{"type":"header","key":"X-User-Type","value":"beta","targetRouteId":"nonexistent"}]}}
                """);
        when(syncService.getRoutesConfig()).thenReturn(AegisRoutesConfig.empty());
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);
        ServerWebExchange exchange = exchange("X-User-Type", "beta");
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.getAttributes()).doesNotContainKey(AegisDiscoveryMetadata.ATTR_KEY);
        verify(chain).filter(exchange);
    }

    @Test
    void filter_shouldUseCustomGovernanceKey() {
        when(syncService.getGovernanceConfigJson()).thenReturn("""
                {"canary":{"rules":[{"type":"header","key":"X-User-Type","value":"beta","targetRouteId":"user-canary"}]}}
                """);
        when(syncService.getRoutesConfig()).thenReturn(routesConfig("user-canary", "prod-canary", "DEFAULT_GROUP"));
        GrayRoutingFilter filter = new GrayRoutingFilter("canary", objectMapper, syncService);
        ServerWebExchange exchange = exchange("X-User-Type", "beta");
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.<AegisDiscoveryMetadata>getAttribute(AegisDiscoveryMetadata.ATTR_KEY))
                .isEqualTo(new AegisDiscoveryMetadata("prod-canary", "DEFAULT_GROUP"));
    }

    @Test
    void getOrder_shouldReturnGrayFilterOrder() {
        when(syncService.getGovernanceConfigJson()).thenReturn("{}");
        when(syncService.getRoutesConfig()).thenReturn(AegisRoutesConfig.empty());
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);

        assertThat(filter.getOrder()).isEqualTo(AegisFilterOrder.GRAY);
    }

    @Test
    void filter_shouldUseUpdatedRulesAfterGovernanceConfigChange() {
        // Initial: rules target "prod-canary"
        ArgumentCaptor<Consumer<String>> listenerCaptor = ArgumentCaptor.forClass(Consumer.class);
        when(syncService.getGovernanceConfigJson()).thenReturn("""
                {"gray":{"rules":[{"type":"header","key":"X-User-Type","value":"beta","targetRouteId":"user-canary"}]}}
                """);
        when(syncService.getRoutesConfig()).thenReturn(routesConfig("user-canary", "prod-canary", "DEFAULT_GROUP"));
        doNothing().when(syncService).registerGovernanceListener(listenerCaptor.capture());
        doNothing().when(syncService).registerRoutesListener(any());
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);

        // Request matches with initial config
        ServerWebExchange exchange1 = exchange("X-User-Type", "beta");
        GatewayFilterChain chain1 = mock(GatewayFilterChain.class);
        when(chain1.filter(exchange1)).thenReturn(Mono.empty());
        StepVerifier.create(filter.filter(exchange1, chain1)).verifyComplete();
        assertThat(exchange1.<AegisDiscoveryMetadata>getAttribute(AegisDiscoveryMetadata.ATTR_KEY))
                .isEqualTo(new AegisDiscoveryMetadata("prod-canary", "DEFAULT_GROUP"));

        // Simulate Nacos push: remove all rules
        listenerCaptor.getValue().accept("{}");

        // Request with same header no longer matches (rules cleared)
        ServerWebExchange exchange2 = exchange("X-User-Type", "beta");
        GatewayFilterChain chain2 = mock(GatewayFilterChain.class);
        when(chain2.filter(exchange2)).thenReturn(Mono.empty());
        StepVerifier.create(filter.filter(exchange2, chain2)).verifyComplete();
        assertThat(exchange2.getAttributes()).doesNotContainKey(AegisDiscoveryMetadata.ATTR_KEY);
    }

    @Test
    void filter_shouldUseUpdatedRouteMapAfterRoutesConfigChange() {
        // Initial: route map with "prod-canary"
        ArgumentCaptor<Consumer<AegisRoutesConfig>> routesListenerCaptor = ArgumentCaptor.forClass(Consumer.class);
        when(syncService.getGovernanceConfigJson()).thenReturn("""
                {"gray":{"rules":[{"type":"header","key":"X-User-Type","value":"beta","targetRouteId":"user-canary"}]}}
                """);
        when(syncService.getRoutesConfig()).thenReturn(routesConfig("user-canary", "prod-canary", "DEFAULT_GROUP"));
        doNothing().when(syncService).registerGovernanceListener(any());
        doNothing().when(syncService).registerRoutesListener(routesListenerCaptor.capture());
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);

        // Simulate route update: user-canary now points to "prod-staging"
        routesListenerCaptor.getValue().accept(routesConfig("user-canary", "prod-staging", "DEFAULT_GROUP"));

        ServerWebExchange exchange = exchange("X-User-Type", "beta");
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());
        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.<AegisDiscoveryMetadata>getAttribute(AegisDiscoveryMetadata.ATTR_KEY))
                .isEqualTo(new AegisDiscoveryMetadata("prod-staging", "DEFAULT_GROUP"));
    }

    @Test
    void filter_shouldApplyFirstMatchingRule() {
        // Two rules: first matches on "beta", second also matches on "beta" but for a different target
        when(syncService.getGovernanceConfigJson()).thenReturn("""
                {"gray":{"rules":[
                    {"type":"header","key":"X-User-Type","value":"beta","targetRouteId":"user-canary-v1"},
                    {"type":"header","key":"X-User-Type","value":"beta","targetRouteId":"user-canary-v2"}
                ]}}
                """);
        when(syncService.getRoutesConfig()).thenReturn(new AegisRoutesConfig(List.of(
                new AegisRoute("user-canary-v1", "lb://user-service", List.of(), List.of(), 0,
                        Map.of("discovery", Map.of("namespace", "ns-v1", "group", "DEFAULT_GROUP"))),
                new AegisRoute("user-canary-v2", "lb://user-service", List.of(), List.of(), 0,
                        Map.of("discovery", Map.of("namespace", "ns-v2", "group", "DEFAULT_GROUP")))
        )));
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);
        ServerWebExchange exchange = exchange("X-User-Type", "beta");
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        // First rule wins
        assertThat(exchange.<AegisDiscoveryMetadata>getAttribute(AegisDiscoveryMetadata.ATTR_KEY))
                .isEqualTo(new AegisDiscoveryMetadata("ns-v1", "DEFAULT_GROUP"));
    }

    @Test
    void filter_shouldRetainPreviousRulesWhenGovernanceUpdateIsInvalidJson() {
        ArgumentCaptor<Consumer<String>> listenerCaptor = ArgumentCaptor.forClass(Consumer.class);
        when(syncService.getGovernanceConfigJson()).thenReturn("""
                {"gray":{"rules":[{"type":"header","key":"X-User-Type","value":"beta","targetRouteId":"user-canary"}]}}
                """);
        when(syncService.getRoutesConfig()).thenReturn(routesConfig("user-canary", "prod-canary", "DEFAULT_GROUP"));
        doNothing().when(syncService).registerGovernanceListener(listenerCaptor.capture());
        doNothing().when(syncService).registerRoutesListener(any());
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);

        // Simulate push of invalid JSON
        listenerCaptor.getValue().accept("not valid json {{{");

        // Previous rules still active
        ServerWebExchange exchange = exchange("X-User-Type", "beta");
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());
        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.<AegisDiscoveryMetadata>getAttribute(AegisDiscoveryMetadata.ATTR_KEY))
                .isEqualTo(new AegisDiscoveryMetadata("prod-canary", "DEFAULT_GROUP"));
    }

    private static AegisRoutesConfig routesConfig(String routeId, String namespace, String group) {
        AegisRoute route = new AegisRoute(
                routeId, "lb://user-service", List.of("Path=/api/**"), List.of(), 0,
                Map.of("discovery", Map.of("namespace", namespace, "group", group)));
        return new AegisRoutesConfig(List.of(route));
    }

    private static ServerWebExchange exchange(String headerName, String headerValue) {
        return MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/test").header(headerName, headerValue).build());
    }
}

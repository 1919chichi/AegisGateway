package io.aegis.gateway.gray.filter;

import io.aegis.gateway.core.filter.AegisFilterOrder;
import io.aegis.gateway.core.model.AegisDiscoveryMetadata;
import io.aegis.gateway.core.model.config.AegisRoute;
import io.aegis.gateway.core.model.config.AegisRoutesConfig;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
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

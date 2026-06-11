package io.aegis.gateway.ratelimit.core;

import io.aegis.gateway.ratelimit.model.RateLimitPolicy;
import io.aegis.gateway.ratelimit.model.RateLimitRule;
import io.aegis.gateway.ratelimit.model.RateLimitType;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimitKeyResolverTest {

    private final RateLimitKeyResolver resolver = new RateLimitKeyResolver();

    @Test
    void shouldResolveServicePathAndUserKeysInUserPathServiceOrder() {
        RateLimitPolicy policy = new RateLimitPolicy("user-service-policy", List.of(
                new RateLimitRule("service-total", RateLimitType.SERVICE, 1000, 500, null, null),
                new RateLimitRule("login-path", RateLimitType.PATH, 50, 10, "/api/users/login", null),
                new RateLimitRule("per-user", RateLimitType.USER, 60, 10, null, "X-User-Id")
        ));
        var exchange = MockServerWebExchange.from(
                MockServerHttpRequest.post("/api/users/login?debug=true").header("X-User-Id", "10001").build());

        List<MatchedRateLimitRule> matched = resolver.resolve(exchange, route("lb://user-service"), policy);

        assertThat(matched).extracting(m -> m.rule().id())
                .containsExactly("per-user", "login-path", "service-total");
        assertThat(matched).extracting(MatchedRateLimitRule::key).containsExactly(
                "aegis:ratelimit:user-service-policy:per-user:10001",
                "aegis:ratelimit:user-service-policy:login-path",
                "aegis:ratelimit:user-service-policy:service-total:user-service"
        );
    }

    @Test
    void shouldSkipPathRuleWhenRequestPathDoesNotMatchPattern() {
        RateLimitPolicy policy = new RateLimitPolicy("user-service-policy", List.of(
                new RateLimitRule("login-path", RateLimitType.PATH, 50, 10, "/api/users/login", null),
                new RateLimitRule("service-total", RateLimitType.SERVICE, 1000, 500, null, null)
        ));
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/api/users/profile").build());

        List<MatchedRateLimitRule> matched = resolver.resolve(exchange, route("lb://user-service"), policy);

        assertThat(matched).extracting(m -> m.rule().id()).containsExactly("service-total");
    }

    @Test
    void shouldSupportSpringPathPatternSyntax() {
        RateLimitPolicy policy = new RateLimitPolicy("order-policy", List.of(
                new RateLimitRule("order-detail", RateLimitType.PATH, 50, 10, "/api/orders/{id}", null)
        ));
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/api/orders/123").build());

        List<MatchedRateLimitRule> matched = resolver.resolve(exchange, route("lb://order-service"), policy);

        assertThat(matched).extracting(m -> m.rule().id()).containsExactly("order-detail");
        assertThat(matched.getFirst().key()).isEqualTo("aegis:ratelimit:order-policy:order-detail");
    }

    @Test
    void shouldUseAnonymousWhenUserHeaderIsMissing() {
        RateLimitPolicy policy = new RateLimitPolicy("user-policy", List.of(
                new RateLimitRule("per-user", RateLimitType.USER, 60, 10, null, null)
        ));
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/api/users/profile").build());

        List<MatchedRateLimitRule> matched = resolver.resolve(exchange, route("lb://user-service"), policy);

        assertThat(matched.getFirst().key()).isEqualTo("aegis:ratelimit:user-policy:per-user:anonymous");
    }

    @Test
    void shouldSanitizeAndTruncateUserIdentityInRedisKey() {
        String unsafe = "{tenant}:user:" + "x".repeat(80);
        RateLimitPolicy policy = new RateLimitPolicy("user-policy", List.of(
                new RateLimitRule("per-user", RateLimitType.USER, 60, 10, null, "X-User-Id")
        ));
        var exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/users/profile").header("X-User-Id", unsafe).build());

        List<MatchedRateLimitRule> matched = resolver.resolve(exchange, route("lb://user-service"), policy);

        String identity = matched.getFirst().key().substring("aegis:ratelimit:user-policy:per-user:".length());
        assertThat(identity).doesNotContain("{", "}", ":");
        assertThat(identity).hasSize(64);
    }

    private static Route route(String uri) {
        return Route.async()
                .id("user-route")
                .uri(uri)
                .predicate(exchange -> true)
                .build();
    }
}

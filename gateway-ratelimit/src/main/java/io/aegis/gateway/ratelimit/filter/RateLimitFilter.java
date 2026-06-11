package io.aegis.gateway.ratelimit.filter;

import io.aegis.gateway.core.exception.ApiErrorResponseWriter;
import io.aegis.gateway.core.filter.AegisFilterOrder;
import io.aegis.gateway.ratelimit.core.MatchedRateLimitRule;
import io.aegis.gateway.ratelimit.core.RateLimitKeyResolver;
import io.aegis.gateway.ratelimit.core.ReactiveRateLimiterGateway;
import io.aegis.gateway.ratelimit.model.RateLimitPolicy;
import io.aegis.gateway.ratelimit.model.RateLimitType;
import io.aegis.gateway.ratelimit.repository.RateLimitPolicyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR;

/**
 * 分布式限流 {@link GlobalFilter}：读取路由 metadata 中的 {@code rateLimit.policyId}，
 * 对命中的规则按 USER → PATH → SERVICE 顺序逐个扣令牌（AND 语义），任一失败返回 429。
 * <p>
 * fail-open 原则：路由未绑定策略、策略不存在、Redis 未配置/不可用、扣令牌出错，一律放行——
 * 限流是保护手段，不能成为新的单点故障。
 * <p>
 * 429 响应必须在此直接写出而非抛异常：{@code GlobalExceptionHandler} 对 429 异常只能
 * 统一映射为 RATE_LIMIT_PATH，无法区分限流维度的业务错误码。
 */
public class RateLimitFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);

    private final RateLimitPolicyRepository repository;
    private final ReactiveRateLimiterGateway limiterGateway;
    private final ObjectMapper objectMapper;
    private final RateLimitKeyResolver keyResolver = new RateLimitKeyResolver();

    public RateLimitFilter(RateLimitPolicyRepository repository,
                           ReactiveRateLimiterGateway limiterGateway,
                           ObjectMapper objectMapper) {
        this.repository = repository;
        this.limiterGateway = limiterGateway;
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        Route route = exchange.getAttribute(GATEWAY_ROUTE_ATTR);
        String policyId = policyId(route);
        if (policyId == null) {
            return chain.filter(exchange);
        }

        return repository.findById(policyId)
                .map(policy -> applyPolicy(exchange, chain, route, policy))
                .orElseGet(() -> {
                    log.warn("Rate limit policy '{}' not found, pass through", policyId);
                    return chain.filter(exchange);
                });
    }

    private Mono<Void> applyPolicy(ServerWebExchange exchange, GatewayFilterChain chain,
                                   Route route, RateLimitPolicy policy) {
        List<MatchedRateLimitRule> matchedRules = keyResolver.resolve(exchange, route, policy);
        if (matchedRules.isEmpty()) {
            return chain.filter(exchange);
        }
        return applyRule(exchange, chain, matchedRules, 0);
    }

    private Mono<Void> applyRule(ServerWebExchange exchange, GatewayFilterChain chain,
                                 List<MatchedRateLimitRule> rules, int index) {
        if (index >= rules.size()) {
            return chain.filter(exchange);
        }
        MatchedRateLimitRule matched = rules.get(index);
        return limiterGateway.tryAcquire(matched.key(), matched.rule())
                .materialize()
                .flatMap(signal -> {
                    if (signal.isOnError()) {
                        log.error("Rate limit check failed, fail open. key={}", matched.key(), signal.getThrowable());
                        return chain.filter(exchange);
                    }
                    if (!signal.hasValue()) {
                        // 空 Mono 是 limiter gateway 的常规信号（Redis 未配置或重连中），
                        // debug 级别即可：状态变化已由 RedissonClientManager 记录，这里不能按请求刷日志
                        log.debug("Rate limiter unavailable, fail open. key={}", matched.key());
                        return chain.filter(exchange);
                    }
                    return signal.get()
                            ? applyRule(exchange, chain, rules, index + 1)
                            : writeRateLimited(exchange, matched.rule().type());
                });
    }

    private Mono<Void> writeRateLimited(ServerWebExchange exchange, RateLimitType type) {
        return ApiErrorResponseWriter.write(
                exchange.getResponse(), HttpStatus.TOO_MANY_REQUESTS, type.errorCode(), objectMapper);
    }

    private String policyId(Route route) {
        if (route == null) {
            return null;
        }
        Object rateLimit = route.getMetadata().get("rateLimit");
        if (!(rateLimit instanceof Map<?, ?> map)) {
            return null;
        }
        Object policyId = map.get("policyId");
        return policyId instanceof String value && !value.isBlank() ? value : null;
    }

    @Override
    public int getOrder() {
        return AegisFilterOrder.RATE_LIMIT;
    }
}

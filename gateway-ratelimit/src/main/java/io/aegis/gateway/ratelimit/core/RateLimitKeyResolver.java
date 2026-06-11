package io.aegis.gateway.ratelimit.core;

import io.aegis.gateway.ratelimit.model.RateLimitPolicy;
import io.aegis.gateway.ratelimit.model.RateLimitRule;
import io.aegis.gateway.ratelimit.model.RateLimitType;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.http.server.PathContainer;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 计算请求命中的限流规则，并为每条命中规则生成 Redis key。
 * <p>
 * 返回顺序固定为 USER → PATH → SERVICE（同类型内按配置顺序）：更细粒度、更易拒绝的规则
 * 先扣令牌，多桶无回滚时被浪费的令牌最少。
 * <p>
 * key 安全约束：dimensionValue 来自配置或请求头，{@code {} } 在 Redis Cluster 下是 hash tag、
 * {@code :} 破坏 key 分段，统一替换为 {@code _}；USER 维度的 identity 是外部输入，截断到
 * {@value #MAX_IDENTITY_LENGTH} 字符防止恶意超长 header 生成巨型 key。
 */
public class RateLimitKeyResolver {

    private static final String KEY_PREFIX = "aegis:ratelimit:";
    private static final int MAX_IDENTITY_LENGTH = 64;

    private final PathPatternParser pathPatternParser = new PathPatternParser();

    /**
     * pathPattern 字符串 → 编译结果的缓存。模式编译有 regex 构建开销，不能在每请求热路径上做；
     * 模式只来自已通过校验的治理配置，集合天然有界（历史上配置过的去重模式数），不会被请求撑大。
     */
    private final Map<String, PathPattern> compiledPatterns = new ConcurrentHashMap<>();

    public List<MatchedRateLimitRule> resolve(ServerWebExchange exchange, Route route, RateLimitPolicy policy) {
        // 请求路径只解析一次，供所有 PATH 规则共用
        PathContainer requestPath = exchange.getRequest().getPath().pathWithinApplication();
        List<MatchedRateLimitRule> matched = new ArrayList<>();
        appendMatchedByType(exchange, route, policy, RateLimitType.USER, requestPath, matched);
        appendMatchedByType(exchange, route, policy, RateLimitType.PATH, requestPath, matched);
        appendMatchedByType(exchange, route, policy, RateLimitType.SERVICE, requestPath, matched);
        return List.copyOf(matched);
    }

    private void appendMatchedByType(ServerWebExchange exchange, Route route, RateLimitPolicy policy,
                                     RateLimitType type, PathContainer requestPath,
                                     List<MatchedRateLimitRule> matched) {
        for (RateLimitRule rule : policy.rules()) {
            if (rule.type() == type && matches(rule, requestPath)) {
                matched.add(new MatchedRateLimitRule(rule, key(exchange, route, policy, rule)));
            }
        }
    }

    private boolean matches(RateLimitRule rule, PathContainer requestPath) {
        if (rule.type() != RateLimitType.PATH) {
            return true;
        }
        return compiledPatterns
                .computeIfAbsent(rule.pathPattern(), pathPatternParser::parse)
                .matches(requestPath);
    }

    private String key(ServerWebExchange exchange, Route route, RateLimitPolicy policy, RateLimitRule rule) {
        String base = KEY_PREFIX + sanitize(policy.id()) + ":" + sanitize(rule.id());
        return switch (rule.type()) {
            // PATH 桶由 ruleId 唯一确定，pathPattern 不进 key（冗余且含 {}: 等不安全字符）
            case PATH -> base;
            case SERVICE -> base + ":" + sanitize(serviceId(route));
            case USER -> base + ":" + userIdentity(exchange, rule);
        };
    }

    private String userIdentity(ServerWebExchange exchange, RateLimitRule rule) {
        String identity = exchange.getRequest().getHeaders().getFirst(rule.identityHeader());
        if (identity == null || identity.isBlank()) {
            identity = "anonymous";
        }
        return truncate(sanitize(identity));
    }

    private static String serviceId(Route route) {
        if (route != null && route.getUri() != null && "lb".equals(route.getUri().getScheme())
                && route.getUri().getHost() != null && !route.getUri().getHost().isBlank()) {
            return route.getUri().getHost();
        }
        return route == null ? "unknown" : route.getId();
    }

    private static String sanitize(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value.replace('{', '_').replace('}', '_').replace(':', '_');
    }

    private static String truncate(String value) {
        return value.length() > MAX_IDENTITY_LENGTH ? value.substring(0, MAX_IDENTITY_LENGTH) : value;
    }
}

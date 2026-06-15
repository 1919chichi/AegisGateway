package io.aegis.gateway.ratelimit.model;

import java.util.List;

/**
 * 限流策略组：路由通过 metadata 绑定 {@code policyId}，规则细节全部在组内维护，
 * 路由配置不随限流细节变化。rules 保持配置顺序（同类型内的执行顺序依赖它）。
 */
public record RateLimitPolicy(String id, List<RateLimitRule> rules) {
    public RateLimitPolicy {
        rules = rules == null ? List.of() : List.copyOf(rules);
    }
}

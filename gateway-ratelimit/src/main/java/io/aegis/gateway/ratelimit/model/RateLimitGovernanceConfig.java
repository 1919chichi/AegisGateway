package io.aegis.gateway.ratelimit.model;

import java.util.List;

/**
 * {@code aegis-governance.json} 中限流模块关心的配置段：策略列表与 Redis 连接配置。
 * <p>
 * 治理配置以原始 JSON 分发给各模块，本 record 只声明限流自己的节点，
 * 其他模块的节点在反序列化时被忽略。
 *
 * @param rateLimitPolicies 限流策略组列表，缺失时视为空（限流整体不生效）
 * @param rateLimitRedis    Redis 连接配置，null 表示未配置——此时所有限流规则 fail-open
 */
public record RateLimitGovernanceConfig(
        List<RateLimitPolicy> rateLimitPolicies,
        RateLimitRedisConfig rateLimitRedis
) {
    public RateLimitGovernanceConfig {
        rateLimitPolicies = rateLimitPolicies == null ? List.of() : List.copyOf(rateLimitPolicies);
    }
}

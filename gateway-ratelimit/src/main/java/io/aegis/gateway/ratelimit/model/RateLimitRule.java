package io.aegis.gateway.ratelimit.model;

/**
 * 单条限流规则，来自 {@code aegis-governance.json}。
 * <p>
 * capacity/refillRate 在每次扣令牌时随 Lua 脚本下发，因此热更新后新请求立即按新参数限流，
 * 无需同步 Redis 侧状态。
 *
 * @param id             规则 ID，策略组内唯一，同时是 Redis key 的组成部分
 * @param type           限流维度
 * @param capacity       令牌桶容量（允许的突发量）
 * @param refillRate     每秒补充令牌数（近似稳定 QPS），补充间隔固定 1 秒
 * @param pathPattern    PATH 规则的 Spring PathPattern，匹配 StripPrefix 前的原始路径
 * @param identityHeader USER 规则的用户标识请求头，缺省 {@value #DEFAULT_IDENTITY_HEADER}
 */
public record RateLimitRule(
        String id,
        RateLimitType type,
        long capacity,
        long refillRate,
        String pathPattern,
        String identityHeader
) {
    public static final String DEFAULT_IDENTITY_HEADER = "X-User-Id";

    public RateLimitRule {
        identityHeader = identityHeader == null || identityHeader.isBlank()
                ? DEFAULT_IDENTITY_HEADER
                : identityHeader;
    }
}

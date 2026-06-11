package io.aegis.gateway.ratelimit.core;

import io.aegis.gateway.ratelimit.model.RateLimitRule;

/** 一条命中当前请求的规则及其已解析好的 Redis key，避免扣令牌阶段重复计算 key。 */
public record MatchedRateLimitRule(RateLimitRule rule, String key) {}

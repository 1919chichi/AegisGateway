package io.aegis.gateway.ratelimit.core;

import io.aegis.gateway.ratelimit.model.RateLimitRule;
import reactor.core.publisher.Mono;

/**
 * 响应式限流后端抽象，隔离 Filter 与具体 Redis 实现。
 * <p>
 * 返回语义：{@code true} 获取到令牌；{@code false} 应拒绝（429）；
 * 空 Mono 表示后端当前无法做出决策（未配置/重连中），调用方必须 fail-open；
 * 错误信号同样按 fail-open 处理。
 */
public interface ReactiveRateLimiterGateway {
    Mono<Boolean> tryAcquire(String key, RateLimitRule rule);
}

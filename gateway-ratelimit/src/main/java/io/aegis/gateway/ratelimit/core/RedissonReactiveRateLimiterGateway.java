package io.aegis.gateway.ratelimit.core;

import io.aegis.gateway.ratelimit.model.RateLimitRule;
import org.redisson.api.RScript;
import org.redisson.api.RedissonReactiveClient;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 通过 Redisson {@code RScriptReactive} 执行 Redis Lua token bucket 的限流网关实现。
 * <p>
 * 不使用 Redisson {@code RRateLimiter}：其只支持 rate + interval 且桶容量等于 rate，
 * 无法表达"突发 capacity、稳定 refillRate/s"的配置语义。Lua 每次请求携带最新的
 * capacity/refillRate 参数，因此配置热更新天然生效，无需维护 Redis 侧参数指纹。
 * <p>
 * 客户端由 {@link RedissonClientManager} 按需提供；客户端缺失（限流未配置 Redis 或
 * 建连失败）时返回空 Mono，表示"无法做出决策"，由调用方按 fail-open 放行。
 */
public class RedissonReactiveRateLimiterGateway implements ReactiveRateLimiterGateway {

    /**
     * 令牌桶 Lua 脚本。时间取 Redis 服务端时钟（毫秒），多网关实例无时钟偏差问题；
     * 每次执行都重设 TTL，冷 key（如长期不活跃的用户桶）到期自动清理。
     */
    private static final String TOKEN_BUCKET_SCRIPT = """
            local capacity = tonumber(ARGV[1])
            local refillRate = tonumber(ARGV[2])
            local requested = tonumber(ARGV[3])
            local ttlMillis = tonumber(ARGV[4])

            local nowParts = redis.call('time')
            local nowMillis = nowParts[1] * 1000 + math.floor(nowParts[2] / 1000)

            local data = redis.call('hmget', KEYS[1], 'tokens', 'timestamp')
            local tokens = tonumber(data[1])
            local timestamp = tonumber(data[2])

            if tokens == nil then
                tokens = capacity
            end
            if timestamp == nil then
                timestamp = nowMillis
            end

            local elapsedMillis = nowMillis - timestamp
            if elapsedMillis < 0 then
                elapsedMillis = 0
            end

            local refill = elapsedMillis * refillRate / 1000
            tokens = math.min(capacity, tokens + refill)

            local allowed = 0
            if tokens >= requested then
                tokens = tokens - requested
                allowed = 1
            end

            redis.call('hset', KEYS[1], 'tokens', tokens, 'timestamp', nowMillis)
            redis.call('pexpire', KEYS[1], ttlMillis)
            return allowed
            """;

    private final RedissonClientManager clientManager;

    public RedissonReactiveRateLimiterGateway(RedissonClientManager clientManager) {
        this.clientManager = clientManager;
    }

    @Override
    public Mono<Boolean> tryAcquire(String key, RateLimitRule rule) {
        RedissonReactiveClient redisson = clientManager.current();
        if (redisson == null) {
            // 客户端缺失时顺带触发带冷却的异步重建，请求本身立即按"无决策"返回
            clientManager.retryLater();
            return Mono.empty();
        }
        return redisson.getScript().eval(
                RScript.Mode.READ_WRITE,
                TOKEN_BUCKET_SCRIPT,
                RScript.ReturnType.BOOLEAN,
                List.of(key),
                rule.capacity(),
                rule.refillRate(),
                1L,
                ttlMillis(rule));
    }

    /**
     * TTL 取 max(桶完整补满周期, 60s)：足够覆盖正常限流窗口，又让冷 key 自动过期，
     * 防止 USER 维度的 key 随用户数无限累积。
     */
    private static long ttlMillis(RateLimitRule rule) {
        long refillSeconds = Math.ceilDiv(rule.capacity(), rule.refillRate());
        return Math.max(60, refillSeconds) * 1000;
    }
}

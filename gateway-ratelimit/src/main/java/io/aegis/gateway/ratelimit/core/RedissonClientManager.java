package io.aegis.gateway.ratelimit.core;

import io.aegis.gateway.ratelimit.model.RateLimitRedisConfig;
import org.redisson.Redisson;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * 按治理配置惰性管理 Redisson 客户端的生命周期，是"网关启动不依赖 Redis"约束的实现核心。
 * <p>
 * 客户端只在"存在限流策略且配置了 {@code rateLimitRedis}"时创建；策略清空或配置移除时关闭。
 * Redisson 不支持在存活客户端上修改服务端地址，因此 Redis 配置热更新通过
 * "创建新客户端 → 原子替换 → 关闭旧客户端"实现，旧客户端上未完成的请求自然结束。
 * <p>
 * 线程模型：{@link #apply} 仅由 Nacos governance 单线程 Executor 调用（虚拟线程，可阻塞建连）；
 * {@link #reconcile} 用 synchronized 串行化，确保与请求路径触发的重试不会并发创建。
 * 请求路径只做 volatile 读（{@link #current}），永不阻塞事件循环。
 * <p>
 * 创建失败（如 Redis 暂时不可用）不会传播异常：保持现状并记录日志，期间限流 fail-open。
 * 自愈通过两条路径：下一次治理配置推送，或请求路径上带冷却时间的 {@link #retryLater}。
 */
public class RedissonClientManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RedissonClientManager.class);

    /** 请求路径重试建连的冷却毫秒数：避免 Redis 故障期间高 QPS 把建连尝试打成风暴。 */
    static final long RETRY_COOLDOWN_MS = 10_000;

    private final Function<RateLimitRedisConfig, RedissonReactiveClient> clientFactory;
    private final long retryCooldownMillis;
    private final AtomicLong lastFailedAttemptMillis = new AtomicLong();

    private volatile RateLimitRedisConfig desiredConfig;
    private volatile RateLimitRedisConfig activeConfig;
    private volatile RedissonReactiveClient client;
    private volatile boolean closed;

    public RedissonClientManager() {
        this(RedissonClientManager::createClient, RETRY_COOLDOWN_MS);
    }

    /** 测试入口：注入假的客户端工厂与冷却时间，避免真实建连和真实等待。 */
    RedissonClientManager(Function<RateLimitRedisConfig, RedissonReactiveClient> clientFactory,
                          long retryCooldownMillis) {
        this.clientFactory = clientFactory;
        this.retryCooldownMillis = retryCooldownMillis;
    }

    /** 请求路径读取当前客户端；返回 null 表示限流未启用或 Redis 暂不可用（调用方应 fail-open）。 */
    public RedissonReactiveClient current() {
        return client;
    }

    /**
     * 治理配置更新入口。{@code rateLimitInUse} 为 false（无任何限流策略）时即使配置了
     * Redis 也不保持连接——这是"配置了限流才使用 Redis"约束的落点。
     */
    public void apply(RateLimitRedisConfig config, boolean rateLimitInUse) {
        if (rateLimitInUse && config == null) {
            log.warn("Rate limit policies exist but 'rateLimitRedis' is missing in governance config, "
                    + "all rate limit rules fail open");
        }
        desiredConfig = rateLimitInUse ? config : null;
        reconcile();
    }

    /**
     * 请求路径发现客户端缺失时调用：若处于"应该有客户端但创建失败"状态且过了冷却期，
     * 在 boundedElastic 上调度一次重建（建连是阻塞操作，不能在事件循环线程上做）。
     *
     * @return 是否实际调度了重试，便于测试验证冷却语义
     */
    public boolean retryLater() {
        if (closed || desiredConfig == null || client != null) {
            return false;
        }
        long last = lastFailedAttemptMillis.get();
        long now = System.currentTimeMillis();
        if (now - last < retryCooldownMillis || !lastFailedAttemptMillis.compareAndSet(last, now)) {
            return false;
        }
        Schedulers.boundedElastic().schedule(this::reconcile);
        return true;
    }

    private synchronized void reconcile() {
        if (closed) {
            return;
        }
        RateLimitRedisConfig desired = desiredConfig;
        if (desired == null || !desired.hasValidAddress()) {
            shutdownCurrent();
            return;
        }
        if (client != null && desired.equals(activeConfig)) {
            return;
        }
        try {
            RedissonReactiveClient created = clientFactory.apply(desired);
            RedissonReactiveClient old = client;
            client = created;
            activeConfig = desired;
            log.info("Redisson client ready for rate limiting, address={}", desired.address());
            if (old != null) {
                old.shutdown();
            }
        } catch (Exception e) {
            // 保留旧客户端（若有）继续服务：旧地址即使已失效，请求也只会走 fail-open，
            // 比立刻丢弃一个可能仍然健康的连接更安全
            lastFailedAttemptMillis.set(System.currentTimeMillis());
            log.error("Failed to create Redisson client for rate limiting, address={}, "
                    + "rate limit rules fail open until reconnect succeeds", desired.address(), e);
        }
    }

    private void shutdownCurrent() {
        RedissonReactiveClient old = client;
        client = null;
        activeConfig = null;
        if (old != null) {
            log.info("Shutting down rate limit Redisson client (rate limiting disabled or redis config removed)");
            old.shutdown();
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        shutdownCurrent();
    }

    private static RedissonReactiveClient createClient(RateLimitRedisConfig config) {
        Config redissonConfig = new Config();
        SingleServerConfig single = redissonConfig.useSingleServer()
                .setAddress(config.address())
                .setDatabase(config.database());
        if (config.password() != null && !config.password().isBlank()) {
            single.setPassword(config.password());
        }
        return Redisson.create(redissonConfig).reactive();
    }
}

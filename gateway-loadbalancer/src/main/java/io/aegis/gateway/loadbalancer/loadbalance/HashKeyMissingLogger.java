package io.aegis.gateway.loadbalancer.loadbalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * 一致性哈希 key 缺失时按 serviceId 限流打印 WARN 日志，避免配置错误
 * （如 header 名写错导致所有请求都缺 key）时刷屏拖累网关。
 * <p>
 * 每个 serviceId 维护一个"上次打印时间"的原子时间戳，距上次打印超过 {@code windowMillis}
 * 才允许再打印一条；{@code compareAndSet} 保证并发请求同时触发时只有一个线程真正打印，
 * 其余线程直接跳过，不需要额外加锁。
 */
final class HashKeyMissingLogger {

    private static final Logger log = LoggerFactory.getLogger(HashKeyMissingLogger.class);

    private static final long DEFAULT_WINDOW_MILLIS = 30_000;

    private final ConcurrentHashMap<String, AtomicLong> lastLoggedAt = new ConcurrentHashMap<>();
    private final long windowMillis;
    private final LongSupplier clock;

    HashKeyMissingLogger() {
        this(DEFAULT_WINDOW_MILLIS, System::currentTimeMillis);
    }

    /** 测试入口：注入更短的窗口和可控时钟，避免真实 sleep 等待。 */
    HashKeyMissingLogger(long windowMillis, LongSupplier clock) {
        this.windowMillis = windowMillis;
        this.clock = clock;
    }

    /**
     * @return 本次调用是否实际打印了日志（供测试验证限流窗口语义，调用方无需关心返回值）
     */
    boolean warnIfDue(String serviceId, LoadBalancePolicy policy) {
        long now = clock.getAsLong();
        // 哨兵值必须保证首次调用一定判定为"已过窗口"：AtomicLong 默认初值 0 在真实时钟
        // （epoch 毫秒，约 1.7e12）下天然满足这一点，但受控测试时钟从 0 开始时会和它撞在
        // 一起，导致第一次调用被误判为"窗口内"而漏打日志。用 Long.MIN_VALUE / 2 兜底，
        // 保证无论时钟从多小的值起算，首次调用的 now - prev 都远大于 windowMillis，
        // 同时足够小以避免 now - prev 在极端情况下溢出。
        AtomicLong last = lastLoggedAt.computeIfAbsent(serviceId, k -> new AtomicLong(Long.MIN_VALUE / 2));
        long prev = last.get();
        if (now - prev >= windowMillis && last.compareAndSet(prev, now)) {
            log.warn("Consistent hash key missing, degraded to round-robin. serviceId={}, keySource={}, keyName={}",
                    serviceId, policy.keySource(), policy.keyName());
            return true;
        }
        return false;
    }
}

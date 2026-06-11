package io.aegis.gateway.ratelimit.core;

import io.aegis.gateway.ratelimit.model.RateLimitRedisConfig;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonReactiveClient;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class RedissonClientManagerTest {

    private static final RateLimitRedisConfig CONFIG_A =
            new RateLimitRedisConfig("redis://127.0.0.1:6379", null, 0);
    private static final RateLimitRedisConfig CONFIG_B =
            new RateLimitRedisConfig("redis://127.0.0.1:6380", null, 0);

    private final RedissonReactiveClient clientA = mock(RedissonReactiveClient.class);
    private final RedissonReactiveClient clientB = mock(RedissonReactiveClient.class);

    @Test
    void shouldNotCreateClientWhenRateLimitIsNotInUse() {
        AtomicInteger creations = new AtomicInteger();
        var manager = manager(config -> { creations.incrementAndGet(); return clientA; });

        manager.apply(CONFIG_A, false);

        assertThat(manager.current()).isNull();
        assertThat(creations).hasValue(0);
    }

    @Test
    void shouldNotCreateClientWhenRedisConfigIsMissing() {
        AtomicInteger creations = new AtomicInteger();
        var manager = manager(config -> { creations.incrementAndGet(); return clientA; });

        manager.apply(null, true);

        assertThat(manager.current()).isNull();
        assertThat(creations).hasValue(0);
    }

    @Test
    void shouldCreateClientWhenPoliciesAndRedisConfigPresent() {
        var manager = manager(config -> clientA);

        manager.apply(CONFIG_A, true);

        assertThat(manager.current()).isSameAs(clientA);
    }

    @Test
    void shouldNotRecreateClientWhenConfigIsUnchanged() {
        AtomicInteger creations = new AtomicInteger();
        var manager = manager(config -> { creations.incrementAndGet(); return clientA; });

        manager.apply(CONFIG_A, true);
        manager.apply(CONFIG_A, true);

        assertThat(creations).hasValue(1);
    }

    @Test
    void shouldSwapClientAndShutdownOldWhenConfigChanges() {
        var manager = manager(config -> CONFIG_A.equals(config) ? clientA : clientB);

        manager.apply(CONFIG_A, true);
        manager.apply(CONFIG_B, true);

        assertThat(manager.current()).isSameAs(clientB);
        verify(clientA).shutdown();
    }

    @Test
    void shouldShutdownClientWhenRateLimitIsDisabled() {
        var manager = manager(config -> clientA);

        manager.apply(CONFIG_A, true);
        manager.apply(CONFIG_A, false);

        assertThat(manager.current()).isNull();
        verify(clientA).shutdown();
    }

    @Test
    void shouldFailOpenWhenClientCreationFails() {
        var manager = manager(config -> { throw new IllegalStateException("redis down"); });

        manager.apply(CONFIG_A, true);

        assertThat(manager.current()).isNull();
    }

    @Test
    void shouldKeepOldClientWhenSwapCreationFails() {
        var manager = manager(config -> {
            if (CONFIG_A.equals(config)) {
                return clientA;
            }
            throw new IllegalStateException("new redis unreachable");
        });

        manager.apply(CONFIG_A, true);
        manager.apply(CONFIG_B, true);

        // 新地址建连失败时保留旧客户端继续服务，而不是立刻丢弃健康连接
        assertThat(manager.current()).isSameAs(clientA);
    }

    @Test
    void retryLaterShouldRecreateClientAfterCreationFailure() {
        AtomicInteger attempts = new AtomicInteger();
        var manager = manager(config -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("redis down");
            }
            return clientA;
        }, 0);

        manager.apply(CONFIG_A, true);
        assertThat(manager.current()).isNull();

        assertThat(manager.retryLater()).isTrue();
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(manager.current()).isSameAs(clientA));
    }

    @Test
    void retryLaterShouldRespectCooldownAndExistingClient() {
        var failing = manager(config -> { throw new IllegalStateException("redis down"); });
        failing.apply(CONFIG_A, true);
        // 刚失败过，处于冷却期内
        assertThat(failing.retryLater()).isFalse();

        var healthy = manager(config -> clientA, 0);
        healthy.apply(CONFIG_A, true);
        // 客户端健在时无需重试
        assertThat(healthy.retryLater()).isFalse();

        var disabled = manager(config -> clientA, 0);
        disabled.apply(null, false);
        // 限流未启用时无需重试
        assertThat(disabled.retryLater()).isFalse();
    }

    @Test
    void closeShouldShutdownClientAndBlockFurtherRetries() {
        var manager = manager(config -> clientA, 0);
        manager.apply(CONFIG_A, true);

        manager.close();

        assertThat(manager.current()).isNull();
        verify(clientA).shutdown();
        assertThat(manager.retryLater()).isFalse();
    }

    private RedissonClientManager manager(Function<RateLimitRedisConfig, RedissonReactiveClient> factory) {
        return manager(factory, RedissonClientManager.RETRY_COOLDOWN_MS);
    }

    private RedissonClientManager manager(Function<RateLimitRedisConfig, RedissonReactiveClient> factory,
                                          long cooldownMillis) {
        return new RedissonClientManager(factory, cooldownMillis);
    }
}

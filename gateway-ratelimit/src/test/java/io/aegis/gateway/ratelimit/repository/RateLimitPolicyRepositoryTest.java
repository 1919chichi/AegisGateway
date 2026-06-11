package io.aegis.gateway.ratelimit.repository;

import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.ratelimit.core.RedissonClientManager;
import io.aegis.gateway.ratelimit.model.RateLimitRedisConfig;
import io.aegis.gateway.ratelimit.model.RateLimitType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.ObjectMapper;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RateLimitPolicyRepositoryTest {

    private static final String STABLE_SNAPSHOT_JSON = """
            {"rateLimitPolicies":[{"id":"stable","rules":[{"id":"rule","type":"SERVICE","capacity":10,"refillRate":5}]}]}
            """;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final NacosConfigSyncService syncService = mock(NacosConfigSyncService.class);
    private final RedissonClientManager clientManager = mock(RedissonClientManager.class);

    private RateLimitPolicyRepository repository;
    private Consumer<String> governanceListener;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        // 初始快照由 NacosConfigSyncService 在注册时回放；测试中直接驱动捕获到的监听器
        repository = new RateLimitPolicyRepository(syncService, objectMapper, clientManager);
        ArgumentCaptor<Consumer<String>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(syncService).registerGovernanceListener(captor.capture());
        governanceListener = captor.getValue();
    }

    @Test
    void shouldParseMultiplePoliciesFromGovernanceJson() {
        governanceListener.accept("""
                {
                  "rateLimitPolicies": [
                    {
                      "id": "user-service-policy",
                      "rules": [
                        {"id":"service-total","type":"SERVICE","capacity":1000,"refillRate":500},
                        {"id":"login-path","type":"PATH","pathPattern":"/api/users/login","capacity":50,"refillRate":10},
                        {"id":"per-user","type":"USER","capacity":60,"refillRate":10,"identityHeader":"X-Account-Id"}
                      ]
                    }
                  ]
                }
                """);

        var policy = repository.findById("user-service-policy").orElseThrow();
        assertThat(policy.rules()).hasSize(3);
        assertThat(policy.rules().get(0).type()).isEqualTo(RateLimitType.SERVICE);
        assertThat(policy.rules().get(1).pathPattern()).isEqualTo("/api/users/login");
        assertThat(policy.rules().get(2).identityHeader()).isEqualTo("X-Account-Id");
    }

    @Test
    void shouldRetainPreviousSnapshotWhenGovernanceJsonIsInvalid() {
        governanceListener.accept(STABLE_SNAPSHOT_JSON);

        governanceListener.accept("not valid json {{{");

        assertThat(repository.findById("stable")).isPresent();
    }

    @Test
    void shouldRetainPreviousSnapshotWhenRuleIsInvalid() {
        governanceListener.accept(STABLE_SNAPSHOT_JSON);

        governanceListener.accept("""
                {"rateLimitPolicies":[{"id":"broken","rules":[{"id":"rule","type":"SERVICE","capacity":0,"refillRate":5}]}]}
                """);

        assertThat(repository.findById("stable")).isPresent();
        assertThat(repository.findById("broken")).isEmpty();
    }

    @Test
    void shouldRejectDuplicateRuleIdsWithinPolicy() {
        governanceListener.accept(STABLE_SNAPSHOT_JSON);

        governanceListener.accept("""
                {"rateLimitPolicies":[{"id":"duplicate","rules":[
                  {"id":"same","type":"SERVICE","capacity":10,"refillRate":5},
                  {"id":"same","type":"USER","capacity":10,"refillRate":5}
                ]}]}
                """);

        assertThat(repository.findById("stable")).isPresent();
        assertThat(repository.findById("duplicate")).isEmpty();
    }

    @Test
    void shouldRetainPreviousSnapshotWhenRedisAddressIsInvalid() {
        governanceListener.accept(STABLE_SNAPSHOT_JSON);

        governanceListener.accept("""
                {"rateLimitPolicies":[{"id":"new","rules":[{"id":"rule","type":"SERVICE","capacity":10,"refillRate":5}]}],
                 "rateLimitRedis":{"address":"localhost:6379"}}
                """);

        assertThat(repository.findById("stable")).isPresent();
        assertThat(repository.findById("new")).isEmpty();
    }

    @Test
    void shouldApplyRedisConfigToClientManagerWhenPoliciesExist() {
        governanceListener.accept("""
                {"rateLimitPolicies":[{"id":"stable","rules":[{"id":"rule","type":"SERVICE","capacity":10,"refillRate":5}]}],
                 "rateLimitRedis":{"address":"redis://127.0.0.1:6379","database":1}}
                """);

        verify(clientManager).apply(new RateLimitRedisConfig("redis://127.0.0.1:6379", null, 1), true);
    }

    @Test
    void shouldDisableClientWhenNoPoliciesConfigured() {
        // 即使配置了 Redis 地址，没有任何限流策略时也不保持 Redis 连接
        governanceListener.accept("""
                {"rateLimitRedis":{"address":"redis://127.0.0.1:6379"}}
                """);

        verify(clientManager).apply(new RateLimitRedisConfig("redis://127.0.0.1:6379", null, 0), false);
    }

    @Test
    void shouldNotTouchClientManagerWhenConfigIsRejected() {
        governanceListener.accept("not valid json {{{");

        verify(clientManager, never()).apply(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyBoolean());
    }
}

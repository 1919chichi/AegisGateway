package io.aegis.gateway.loadbalancer.loadbalance;

import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.ObjectMapper;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class LoadBalancePolicyRepositoryTest {

    private static final String STABLE_SNAPSHOT_JSON = """
            {"loadBalancePolicies":[{"serviceId":"stable","strategy":"CONSISTENT_HASH","keySource":"CLIENT_IP"}]}
            """;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final NacosConfigSyncService syncService = mock(NacosConfigSyncService.class);

    private LoadBalancePolicyRepository repository;
    private Consumer<String> governanceListener;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        // 初始快照由 NacosConfigSyncService 在注册时回放；测试中直接驱动捕获到的监听器
        repository = new LoadBalancePolicyRepository(syncService, objectMapper);
        ArgumentCaptor<Consumer<String>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(syncService).registerGovernanceListener(captor.capture());
        governanceListener = captor.getValue();
    }

    @Test
    void shouldParsePolicyFromGovernanceJson() {
        governanceListener.accept("""
                {
                  "loadBalancePolicies": [
                    {
                      "serviceId": "order-service",
                      "strategy": "CONSISTENT_HASH",
                      "keySource": "HEADER",
                      "keyName": "X-User-Id",
                      "virtualNodesPerWeight": 200
                    }
                  ]
                }
                """);

        LoadBalancePolicy policy = repository.findByServiceId("order-service").orElseThrow();
        assertThat(policy.strategy()).isEqualTo(LoadBalanceStrategy.CONSISTENT_HASH);
        assertThat(policy.keySource()).isEqualTo(HashKeySource.HEADER);
        assertThat(policy.keyName()).isEqualTo("X-User-Id");
        assertThat(policy.virtualNodesPerWeight()).isEqualTo(200);
    }

    @Test
    void shouldRetainPreviousSnapshotWhenGovernanceJsonIsInvalid() {
        governanceListener.accept(STABLE_SNAPSHOT_JSON);

        governanceListener.accept("not valid json {{{");

        assertThat(repository.findByServiceId("stable")).isPresent();
    }

    @Test
    void shouldRejectDuplicateServiceIdsAndRetainPreviousSnapshot() {
        governanceListener.accept(STABLE_SNAPSHOT_JSON);

        governanceListener.accept("""
                {"loadBalancePolicies":[
                  {"serviceId":"dup","strategy":"CONSISTENT_HASH","keySource":"CLIENT_IP"},
                  {"serviceId":"dup","strategy":"CONSISTENT_HASH","keySource":"CLIENT_IP"}
                ]}
                """);

        assertThat(repository.findByServiceId("stable")).isPresent();
        assertThat(repository.findByServiceId("dup")).isEmpty();
    }

    @Test
    void shouldRejectHeaderSourceWithoutKeyNameAndRetainPreviousSnapshot() {
        governanceListener.accept(STABLE_SNAPSHOT_JSON);

        governanceListener.accept("""
                {"loadBalancePolicies":[
                  {"serviceId":"broken","strategy":"CONSISTENT_HASH","keySource":"HEADER"}
                ]}
                """);

        assertThat(repository.findByServiceId("stable")).isPresent();
        assertThat(repository.findByServiceId("broken")).isEmpty();
    }

    @Test
    void shouldRejectNonPositiveVirtualNodesPerWeightAndRetainPreviousSnapshot() {
        governanceListener.accept(STABLE_SNAPSHOT_JSON);

        governanceListener.accept("""
                {"loadBalancePolicies":[
                  {"serviceId":"broken","strategy":"CONSISTENT_HASH","keySource":"CLIENT_IP","virtualNodesPerWeight":0}
                ]}
                """);

        assertThat(repository.findByServiceId("stable")).isPresent();
        assertThat(repository.findByServiceId("broken")).isEmpty();
    }

    @Test
    void findByServiceId_shouldReturnEmpty_forBlankOrUnknownServiceId() {
        governanceListener.accept(STABLE_SNAPSHOT_JSON);

        assertThat(repository.findByServiceId("")).isEmpty();
        assertThat(repository.findByServiceId(null)).isEmpty();
        assertThat(repository.findByServiceId("unknown-service")).isEmpty();
    }
}

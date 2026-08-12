package io.aegis.gateway.loadbalancer.loadbalance;

import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 监听 Nacos 治理配置中的 {@code loadBalancePolicies} 节点，维护按 serviceId 索引的一致性
 * 哈希 policy 内存快照。结构镜像 {@code RateLimitPolicyRepository}
 * （见 {@code gateway-ratelimit} commit {@code 5e2f3cc}）：同样的"governance policy +
 * Nacos 热更新 + fail-open"范式。
 * <p>
 * 初始快照由 {@link NacosConfigSyncService} 在注册监听器时回放，不要在构造器里自行 get
 * 初始值——那种模式与并发到达的 Nacos 推送存在新值被旧快照覆盖的竞态。
 * <p>
 * 配置整体校验失败（JSON 非法、字段非法）时保留旧快照，坏配置不会打掉正在生效的路由策略，
 * 也不影响批次中其他 serviceId 的有效 policy。
 */
public class LoadBalancePolicyRepository {

    private static final Logger log = LoggerFactory.getLogger(LoadBalancePolicyRepository.class);

    private final ObjectMapper objectMapper;
    private final AtomicReference<Map<String, LoadBalancePolicy>> policies = new AtomicReference<>(Map.of());

    public LoadBalancePolicyRepository(NacosConfigSyncService syncService, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        syncService.registerGovernanceListener(this::onGovernanceUpdate);
    }

    public Optional<LoadBalancePolicy> findByServiceId(String serviceId) {
        if (serviceId == null || serviceId.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(policies.get().get(serviceId));
    }

    private void onGovernanceUpdate(String json) {
        try {
            LoadBalanceGovernanceConfig config = objectMapper.readValue(json, LoadBalanceGovernanceConfig.class);
            validate(config);
            Map<String, LoadBalancePolicy> snapshot = config.loadBalancePolicies().stream()
                    .collect(Collectors.toUnmodifiableMap(LoadBalancePolicy::serviceId, Function.identity()));
            policies.set(snapshot);
        } catch (Exception e) {
            log.error("Failed to parse load balance policies, keep previous snapshot", e);
        }
    }

    private void validate(LoadBalanceGovernanceConfig config) {
        Set<String> serviceIds = new HashSet<>();
        for (LoadBalancePolicy policy : config.loadBalancePolicies()) {
            requireText(policy.serviceId(), "load balance policy serviceId must not be blank");
            if (!serviceIds.add(policy.serviceId())) {
                throw new IllegalArgumentException("Duplicate load balance policy serviceId: " + policy.serviceId());
            }
            if (policy.strategy() == null) {
                throw new IllegalArgumentException("load balance policy strategy must not be null: " + policy.serviceId());
            }
            if (policy.keySource() == null) {
                throw new IllegalArgumentException("load balance policy keySource must not be null: " + policy.serviceId());
            }
            if (policy.keySource() == HashKeySource.HEADER) {
                requireText(policy.keyName(), "keyName must not be blank when keySource=HEADER: " + policy.serviceId());
            }
            if (policy.virtualNodesPerWeight() != null
                    && (policy.virtualNodesPerWeight() <= 0
                        || policy.virtualNodesPerWeight() > LoadBalancePolicy.MAX_VIRTUAL_NODES_PER_WEIGHT)) {
                throw new IllegalArgumentException(
                        "virtualNodesPerWeight must be in (0, " + LoadBalancePolicy.MAX_VIRTUAL_NODES_PER_WEIGHT
                                + "]: " + policy.serviceId());
            }
        }
    }

    private static void requireText(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
    }
}

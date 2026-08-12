package io.aegis.gateway.loadbalancer.loadbalance;

import java.util.List;

/**
 * {@code aegis-governance.json} 中一致性哈希负载均衡模块关心的配置段。
 * <p>
 * 治理配置以原始 JSON 分发给各模块，本 record 只声明自己的 {@code loadBalancePolicies}
 * 节点，其他模块（如 {@code rateLimitPolicies}）的节点在反序列化时被忽略。
 *
 * @param loadBalancePolicies 一致性哈希 policy 列表，缺失时视为空（没有任何服务启用一致性哈希）
 */
public record LoadBalanceGovernanceConfig(List<LoadBalancePolicy> loadBalancePolicies) {
    public LoadBalanceGovernanceConfig {
        loadBalancePolicies = loadBalancePolicies == null ? List.of() : List.copyOf(loadBalancePolicies);
    }
}

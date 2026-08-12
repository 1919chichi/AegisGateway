package io.aegis.gateway.loadbalancer.loadbalance;

/**
 * {@code aegis-governance.json} 中 {@code loadBalancePolicies[].strategy} 字段的取值。
 * 目前只有一种策略；新增策略时 {@link ConsistentHashReactiveLoadBalancer} 及其 Bean
 * 装配需要按 strategy 分支处理，不能假设全部 policy 都是一致性哈希。
 */
public enum LoadBalanceStrategy {
    /** 一致性哈希 + 虚拟节点（Ketama 风格），第一版唯一支持的策略。 */
    CONSISTENT_HASH
}

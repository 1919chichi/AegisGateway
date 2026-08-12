package io.aegis.gateway.loadbalancer.loadbalance;

/**
 * {@code aegis-governance.json} 中 {@code loadBalancePolicies} 数组的单条 policy，按
 * {@code serviceId} 直接索引（不像 {@code RateLimitPolicy} 那样通过路由 metadata 里的
 * policyId 间接引用）——因为 Spring Cloud LoadBalancer 本身就是按 serviceId 一对一装配
 * {@code ReactiveLoadBalancer} Bean 的，没有"多个路由共享同一份负载均衡配置"的场景需要
 * 间接层。
 *
 * @param serviceId              绑定的 Nacos 服务名，非空且在同一批配置内唯一
 * @param strategy               负载均衡策略，非空
 * @param keySource              哈希 key 来源，非空
 * @param keyName                {@code keySource == HEADER} 时必填的请求头名；
 *                               {@code keySource == CLIENT_IP} 时忽略
 * @param virtualNodesPerWeight  每权重虚拟节点数，为 {@code null} 时使用
 *                               {@link #DEFAULT_VIRTUAL_NODES_PER_WEIGHT}；提供时必须在
 *                               (0, {@link #MAX_VIRTUAL_NODES_PER_WEIGHT}] 区间内
 */
public record LoadBalancePolicy(
        String serviceId,
        LoadBalanceStrategy strategy,
        HashKeySource keySource,
        String keyName,
        Integer virtualNodesPerWeight
) {
    /**
     * 每权重虚拟节点数的默认值。160 是 Ketama 一致性哈希的常见经验值：虚拟节点数越多，
     * 环上分布越均匀，但构建/查找成本也越高；160 在均匀性和构建开销之间是被广泛采用的折中。
     */
    public static final int DEFAULT_VIRTUAL_NODES_PER_WEIGHT = 160;

    /**
     * 单个实例允许分配的虚拟节点数上限，两处强制生效：{@code LoadBalancePolicyRepository}
     * 校验 {@code virtualNodesPerWeight} 本身不能超过这个值；
     * {@link io.aegis.gateway.loadbalancer.hash.ConsistentHashRing#build} 在
     * {@code virtualNodesPerWeight × weight} 算出最终节点数之后再兜底一次——weight 来自
     * Nacos 实例 metadata，不受前一处 policy 校验约束，仍可能把乘积重新推高。
     * 100,000 远高于默认值 160 的正常使用范围，但足以避免误配置（如 weight 或
     * virtualNodesPerWeight 被设成异常大的值）在环构建时分配过多节点拖垮 CPU/内存。
     */
    public static final int MAX_VIRTUAL_NODES_PER_WEIGHT = 100_000;
}

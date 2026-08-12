package io.aegis.gateway.loadbalancer.hash;

import org.springframework.cloud.client.ServiceInstance;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 一致性哈希环（Ketama 风格：虚拟节点 + 有序结构）。
 * <p>
 * 每个物理实例按 {@code virtualNodesPerWeight × 实例权重} 分配虚拟节点，虚拟节点位置由
 * {@link MurmurHash3} 对 {@code "host:port-i"} 做哈希得到；用 host:port（而非 Nacos
 * instanceId 或对象引用）作为实例身份参与哈希，保证同一物理实例在不同环重建之间的虚拟
 * 节点位置稳定，不随对象重新构造而漂移。
 * <p>
 * 环只从调用时传入的 {@code instances} 构建，不做任何"过滤已下线实例"的运行时判断——这是
 * 一致性哈希"候选实例不可用时环上查找"需求的实现方式：被移除的实例根本不会出现在新环里，
 * 原本落在它虚拟节点区间的 key 在重建后自然计算到顺时针最近的、仍然存在的虚拟节点上，
 * 不需要额外的可用性检查分支。
 */
public final class ConsistentHashRing {

    /**
     * Nacos {@code NacosServiceDiscovery.hostToServiceInstance()} 转换实例时写入
     * metadata 的权重键，值为 {@code String.valueOf(double)} 形式（如 {@code "1.0"}）。
     * {@link ServiceInstance} 接口本身没有 {@code getWeight()}，这个 metadata 键是
     * 当前依赖版本下读取 Nacos 实例权重的唯一入口。
     */
    static final String NACOS_WEIGHT_METADATA_KEY = "nacos.weight";

    // 固定 seed：保证同一版本算法下环上位置稳定可复现；修改该值等价于对所有 key 做一次
    // 全量重新分布（相当于一次隐式的全量迁移），因此刻意不作为可配置项。
    private static final int SEED = 0;

    private final TreeMap<Long, ServiceInstance> ring;

    private ConsistentHashRing(TreeMap<Long, ServiceInstance> ring) {
        this.ring = ring;
    }

    public static ConsistentHashRing build(List<ServiceInstance> instances, int virtualNodesPerWeight) {
        TreeMap<Long, ServiceInstance> ring = new TreeMap<>();
        for (ServiceInstance instance : instances) {
            double weight = resolveWeight(instance);
            int virtualNodes = (int) Math.round(virtualNodesPerWeight * weight);
            if (virtualNodes <= 0) {
                // weight=0 是 Nacos 语义上"不接收流量"的实例，此处自然表现为不分配虚拟节点，
                // 不需要额外的显式跳过分支去特殊处理
                continue;
            }
            String identity = instance.getHost() + ":" + instance.getPort();
            for (int i = 0; i < virtualNodes; i++) {
                ring.put(position(identity + "-" + i), instance);
            }
        }
        return new ConsistentHashRing(ring);
    }

    /** 顺时针查找离 key 最近的虚拟节点；环为空时返回空，调用方据此降级为轮询或返回 EmptyResponse。 */
    public Optional<ServiceInstance> route(String key) {
        if (ring.isEmpty()) {
            return Optional.empty();
        }
        long position = position(key);
        Map.Entry<Long, ServiceInstance> entry = ring.ceilingEntry(position);
        if (entry == null) {
            entry = ring.firstEntry(); // wrap-around：key 落在环上最后一个虚拟节点之后
        }
        return Optional.of(entry.getValue());
    }

    /** 读取 Nacos 实例权重；metadata 缺失该键或值无法解析为 double 时按等权 1.0 处理。 */
    public static double resolveWeight(ServiceInstance instance) {
        String raw = instance.getMetadata().get(NACOS_WEIGHT_METADATA_KEY);
        if (raw == null || raw.isBlank()) {
            return 1.0;
        }
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            return 1.0;
        }
    }

    /** 仅供本包内测试使用：环上虚拟节点总数。 */
    int virtualNodeCount() {
        return ring.size();
    }

    /** 仅供本包内测试使用：统计属于某个实例的虚拟节点数量，用于验证权重比例分配是否精确。 */
    long virtualNodeCountFor(ServiceInstance instance) {
        return ring.values().stream().filter(v -> v.equals(instance)).count();
    }

    // 归一化到无符号 32 位空间 [0, 2^32)：Ketama 环的常见约定，避免带符号 int 的负数区间
    // 打乱"环位置"的直觉理解（不影响正确性，因为 ceilingEntry/firstEntry 的相对顺序不变）。
    private static long position(String key) {
        return MurmurHash3.hash(key.getBytes(StandardCharsets.UTF_8), SEED) & 0xFFFFFFFFL;
    }
}

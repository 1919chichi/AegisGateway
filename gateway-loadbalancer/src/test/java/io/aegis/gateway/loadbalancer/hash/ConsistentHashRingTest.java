package io.aegis.gateway.loadbalancer.hash;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ConsistentHashRingTest {

    @Test
    void build_shouldAllocateVirtualNodesProportionalToWeight() {
        ServiceInstance defaultWeight = instance("10.0.0.1", 8080, null);
        ServiceInstance heavyWeight = instance("10.0.0.2", 8080, "2.5");

        ConsistentHashRing ring = ConsistentHashRing.build(List.of(defaultWeight, heavyWeight), 10);

        // 缺省 metadata 时按 1.0 等权：10 * 1.0 = 10；heavyWeight 为 10 * 2.5 = 25，两者都是精确整数，不涉及舍入歧义
        assertThat(ring.virtualNodeCountFor(defaultWeight)).isEqualTo(10);
        assertThat(ring.virtualNodeCountFor(heavyWeight)).isEqualTo(25);
        assertThat(ring.virtualNodeCount()).isEqualTo(35);
    }

    @Test
    void route_shouldReturnSameInstance_forSameKey_acrossMultipleCalls() {
        ConsistentHashRing ring = ConsistentHashRing.build(
                List.of(instance("10.0.0.1", 8080, null), instance("10.0.0.2", 8080, null)), 160);

        Optional<ServiceInstance> first = ring.route("user-42");
        Optional<ServiceInstance> second = ring.route("user-42");

        assertThat(first).isPresent();
        assertThat(second).isEqualTo(first);
    }

    @Test
    void route_shouldReturnEmpty_whenInstanceListIsEmpty() {
        ConsistentHashRing ring = ConsistentHashRing.build(List.of(), 160);

        assertThat(ring.route("any-key")).isEmpty();
    }

    @Test
    void route_shouldOnlyRemapKeysThatOriginallyHitRemovedInstance() {
        // 场景已用参考实现模拟过：5 实例、160 虚拟节点/权重、10000 个 key、移除下标 2（10.0.0.3），
        // 实测变化比例 20.04%，落在 [0.15, 0.30] 区间内，且 0 例违反"变化的 key 必然原属于被移除实例"。
        List<ServiceInstance> initialInstances = List.of(
                instance("10.0.0.1", 8080, null),
                instance("10.0.0.2", 8080, null),
                instance("10.0.0.3", 8080, null),
                instance("10.0.0.4", 8080, null),
                instance("10.0.0.5", 8080, null));
        ConsistentHashRing initialRing = ConsistentHashRing.build(initialInstances, 160);

        // 10000 个固定顺序的 key：MurmurHash3 已经把它们打散到环上，不需要真随机数来获得
        // "任意" key 集合，用顺序字符串换取测试可重放（同一输入永远得到同一结果）
        List<String> keys = new ArrayList<>(10_000);
        for (int i = 0; i < 10_000; i++) {
            keys.add("key-" + i);
        }
        Map<String, ServiceInstance> initialMapping = new HashMap<>();
        for (String key : keys) {
            initialMapping.put(key, initialRing.route(key).orElseThrow());
        }

        ServiceInstance removed = initialInstances.get(2); // 10.0.0.3
        List<ServiceInstance> remainingInstances = new ArrayList<>(initialInstances);
        remainingInstances.remove(removed);
        ConsistentHashRing rebuiltRing = ConsistentHashRing.build(remainingInstances, 160);

        int changed = 0;
        for (String key : keys) {
            ServiceInstance before = initialMapping.get(key);
            ServiceInstance after = rebuiltRing.route(key).orElseThrow();
            if (before.equals(after)) {
                // 未变化的 key：不需要额外断言。数学上，"变化 ⟹ 原目标是被移除实例" 与
                // "原目标不是被移除实例 ⟹ 不变化" 互为逆否命题，下面的循环对所有 key 无遗漏地
                // 验证前者，等价于同时验证了后者——不需要再单独写一个"未变化"分支的断言。
                continue;
            }
            changed++;
            assertThat(before).isEqualTo(removed);
        }
        double changeRatio = changed / (double) keys.size();
        assertThat(changeRatio).isBetween(0.15, 0.30);
    }

    private static ServiceInstance instance(String host, int port, String weightMetadata) {
        Map<String, String> metadata = weightMetadata == null ? Map.of() : Map.of("nacos.weight", weightMetadata);
        return new DefaultServiceInstance("instance-" + host, "test-service", host, port, false, metadata);
    }
}

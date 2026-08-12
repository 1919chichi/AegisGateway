package io.aegis.gateway.loadbalancer.hash;

import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicy;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void route_shouldReturnSameIdentity_forSameKey_acrossMultipleCalls() {
        ConsistentHashRing ring = ConsistentHashRing.build(
                List.of(instance("10.0.0.1", 8080, null), instance("10.0.0.2", 8080, null)), 160);

        Optional<String> first = ring.route("user-42");
        Optional<String> second = ring.route("user-42");

        assertThat(first).isPresent();
        assertThat(second).isEqualTo(first);
    }

    @Test
    void route_shouldReturnEmpty_whenInstanceListIsEmpty() {
        ConsistentHashRing ring = ConsistentHashRing.build(List.of(), 160);

        assertThat(ring.route("any-key")).isEmpty();
    }

    @Test
    void resolveWeight_shouldFallbackTo1_0_whenWeightMetadataIsInvalid() {
        // 非法数字字符串
        ServiceInstance malformedWeight = instance("10.0.0.1", 8080, "abc");
        // 空白字符串
        ServiceInstance blankWeight = instance("10.0.0.2", 8080, "");

        ConsistentHashRing ring = ConsistentHashRing.build(
                List.of(malformedWeight, blankWeight), 10);

        // 两个实例都按缺省权重 1.0 处理：10 * 1.0 = 10 虚拟节点
        assertThat(ring.virtualNodeCountFor(malformedWeight)).isEqualTo(10);
        assertThat(ring.virtualNodeCountFor(blankWeight)).isEqualTo(10);
        assertThat(ring.virtualNodeCount()).isEqualTo(20);
    }

    @Test
    void build_shouldThrow_whenComputedVirtualNodesExceedMax() {
        // weight 不受 LoadBalancePolicyRepository 的 virtualNodesPerWeight 校验约束
        // （来自 Nacos 实例 metadata），极端权重仍能把乘积重新推高，build() 必须在
        // 乘积算出来之后做最终兜底
        ServiceInstance extremeWeight = instance("10.0.0.1", 8080, "1000000");

        assertThatThrownBy(() -> ConsistentHashRing.build(
                List.of(extremeWeight), LoadBalancePolicy.DEFAULT_VIRTUAL_NODES_PER_WEIGHT))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void resolveWeight_shouldFallbackTo1_0_whenWeightMetadataIsNonFiniteOrNegative() {
        ServiceInstance nanWeight = instance("10.0.0.1", 8080, "NaN");
        ServiceInstance infiniteWeight = instance("10.0.0.2", 8080, "Infinity");
        ServiceInstance negativeWeight = instance("10.0.0.3", 8080, "-1.0");

        assertThat(ConsistentHashRing.resolveWeight(nanWeight)).isEqualTo(1.0);
        assertThat(ConsistentHashRing.resolveWeight(infiniteWeight)).isEqualTo(1.0);
        assertThat(ConsistentHashRing.resolveWeight(negativeWeight)).isEqualTo(1.0);
    }

    @Test
    void build_shouldReturnEmptyRing_whenAllInstancesHaveZeroWeight() {
        // 所有实例权重都是 0
        ServiceInstance zeroWeightA = instance("10.0.0.1", 8080, "0");
        ServiceInstance zeroWeightB = instance("10.0.0.2", 8080, "0.0");

        ConsistentHashRing ring = ConsistentHashRing.build(
                List.of(zeroWeightA, zeroWeightB), 160);

        // 权重为 0 的实例不分配虚拟节点，环为空
        assertThat(ring.virtualNodeCount()).isEqualTo(0);
        // 空环返回空
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
        Map<String, String> initialMapping = new HashMap<>();
        for (String key : keys) {
            initialMapping.put(key, initialRing.route(key).orElseThrow());
        }

        ServiceInstance removed = initialInstances.get(2); // 10.0.0.3
        String removedIdentity = ConsistentHashRing.identity(removed);
        List<ServiceInstance> remainingInstances = new ArrayList<>(initialInstances);
        remainingInstances.remove(removed);
        ConsistentHashRing rebuiltRing = ConsistentHashRing.build(remainingInstances, 160);

        int changed = 0;
        for (String key : keys) {
            String before = initialMapping.get(key);
            String after = rebuiltRing.route(key).orElseThrow();
            if (before.equals(after)) {
                // 未变化的 key：不需要额外断言。数学上，"变化 ⟹ 原目标是被移除实例" 与
                // "原目标不是被移除实例 ⟹ 不变化" 互为逆否命题，下面的循环对所有 key 无遗漏地
                // 验证前者，等价于同时验证了后者——不需要再单独写一个"未变化"分支的断言。
                continue;
            }
            changed++;
            assertThat(before).isEqualTo(removedIdentity);
        }
        double changeRatio = changed / (double) keys.size();
        assertThat(changeRatio).isBetween(0.15, 0.30);
    }

    private static ServiceInstance instance(String host, int port, String weightMetadata) {
        Map<String, String> metadata = weightMetadata == null ? Map.of() : Map.of("nacos.weight", weightMetadata);
        return new DefaultServiceInstance("instance-" + host, "test-service", host, port, false, metadata);
    }
}

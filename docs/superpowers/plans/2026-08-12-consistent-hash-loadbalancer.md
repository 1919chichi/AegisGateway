---
change: add-consistent-hash-loadbalancer
design-doc: docs/superpowers/specs/2026-08-12-consistent-hash-loadbalancer-design.md
base-ref: bdb48a6ab57c8b3a49c03f2398c5c1ae6d2f8759
---

# 一致性哈希负载均衡 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `gateway-loadbalancer` 模块新增一种可按 serviceId 配置的一致性哈希负载均衡策略（哈希环 + 虚拟节点 + 权重），通过 `aegis-governance.json` 的 `loadBalancePolicies` 节点热更新，未配置的服务行为不变（仍为 SCG 默认轮询）。

**Architecture:** 新增两个子包：`hash`（无状态哈希环算法 `MurmurHash3` + `ConsistentHashRing` + key 提取 `HashKeyExtractor`）与 `loadbalance`（governance policy 模型/仓库 + `ConsistentHashReactiveLoadBalancer` 实现 `ReactiveLoadBalancer<ServiceInstance>`）。`ConsistentHashReactiveLoadBalancer` 内部持有一个 `RoundRobinLoadBalancer` 作为降级委托对象，policy 缺失或 key 提取失败时直接委托给它，不引入额外分支逻辑。环用 Caffeine `Cache<String, ConsistentHashRing>`（`maximumSize(2)`）按"实例集合+权重+虚拟节点数"字符串做缓存 key，命中直接复用、未命中才重建，避免每次请求都重建整个环。

**Tech Stack:** Java 25（`--enable-preview`）、Spring Cloud Gateway WebFlux、Spring Cloud LoadBalancer 5.0.1（`ReactiveLoadBalancer<ServiceInstance>` 扩展点）、Nacos discovery 2025.1.0.0、Caffeine（模块已声明依赖，此前从未使用）、JUnit 5 + Mockito + AssertJ + Reactor `StepVerifier`。

## Global Constraints

- 所有编译/测试任务需要 `--enable-preview`（根 `build.gradle` 已统一配置，不要移除）——见 `docs/rules/build-commands.md`。
- Nacos 是配置唯一来源；`loadBalancePolicies` 通过 `NacosConfigSyncService.registerGovernanceListener()` 热更新，**不要在构造器里自行 get 初值**（与并发 Nacos 推送有覆盖竞态，`RateLimitPolicyRepository` 类注释已明确记录此教训，见 `gateway-ratelimit/src/main/java/io/aegis/gateway/ratelimit/repository/RateLimitPolicyRepository.java:27-28`）。
- 失败一律 fail-open：policy 解析/校验失败保留旧快照；policy 缺失或 key 提取失败降级为轮询；候选实例为空返回空环（`EmptyResponse`）。任何分支都不能抛异常中断请求。
- 每次编写/修改代码必须同步添加/更新注释；公共类需要 Javadoc；注释解释"为什么"不解释"是什么"——见 `docs/rules/comment-conventions.md`。
- 新代码风格镜像 `gateway-ratelimit` 最近落地的 policy-based 限流实现（`RateLimitGovernanceConfig`/`RateLimitPolicy`/`RateLimitPolicyRepository`，commit `5e2f3cc`），以及本模块已有的 `AegisNamespaceLoadBalancerClientConfiguration` / `NamespaceAwareNacosServiceInstanceListSupplier`。
- 以 **design doc** 为准：本计划的组件划分、类结构、缓存策略（Caffeine `Cache<String, ConsistentHashRing>`）均按 `docs/superpowers/specs/2026-08-12-consistent-hash-loadbalancer-design.md` 展开，不采用 `tasks.md` 中"实例集合签名 + 原子替换"的早期措辞。

## 实现前置说明（已核实，直接采信，无需在任务中重新调研）

以下几点是在制定本计划期间通过反编译实际依赖 jar（`spring-cloud-loadbalancer:5.0.1`、`spring-cloud-commons:5.0.1`、`spring-cloud-starter-alibaba-nacos-discovery:2025.1.0.0`、`spring-cloud-context:5.0.1`）以及运行参考实现交叉验证得到的具体事实，design doc 对这几点的描述比较概括，实现时请按这里的精确版本执行：

1. **Nacos 实例权重的读取方式**：`NacosServiceDiscovery.hostToServiceInstance()` 会把权重写入 `ServiceInstance.getMetadata()` 的 `"nacos.weight"` 键，值是 `String.valueOf(instance.getWeight())`（即字符串形式的 double，如 `"1.0"`）。`ServiceInstance` 接口本身和 `NacosServiceInstance` 均**没有** `getWeight()` 方法，只能走 metadata 这条路径。
2. **CLIENT_IP 的取值来源只能是 `X-Forwarded-For` 请求头**：`RequestData`（`org.springframework.cloud.client.loadbalancer.RequestData`，5.0.1 版本）没有 `getRemoteAddress()` 或任何暴露原始连接远端地址的方法（已用 `javap -p` 核实其全部公开/私有成员），只保留了 `HttpMethod`、`URI`、`HttpHeaders`、cookies、attributes。design doc 里"从 RequestData 的 remote address / X-Forwarded-For 取值"这句里的"remote address"分支在当前依赖版本下不存在，因此 `CLIENT_IP` 提取器只能读 `X-Forwarded-For` 头（取第一个逗号分隔值，无该头则返回空触发降级）。
3. **手写 MurmurHash3（32-bit x86 变体）算法已交叉验证正确**：算法结构与业界标准实现完全一致，已用 Python `mmh3` 参考库验证过下面这组固定输入的期望输出（覆盖 4 字节对齐边界的全部尾部分支 rem=0/1/2/3），Task 1 直接使用这些验证过的期望值写测试：

   | 输入（UTF-8） | seed | 期望值（int） |
   |---|---|---|
   | `""`（空数组） | 0 | `0` |
   | `"ab"` | 0 | `-1681926305` |
   | `"abc"` | 0 | `-1277324294` |
   | `"test"` | 0 | `-1167338989` |
   | `"hello"` | 0 | `613153351` |
   | `"test"` | 42 | `-335093414` |

4. **确定性重映射测试用例已用参考实现模拟过，不会 flaky**：Task 2 里"5 实例、10000 key、移除 1 个实例"的具体场景（实例 `10.0.0.1..5:8080`，`virtualNodesPerWeight=160`，key 为 `"key-0".."key-9999"`，移除 `10.0.0.3:8080`）已经用 Python 参考实现跑过一遍，实测变化比例为 **20.04%**（落在 design doc 要求的 15%~30% 区间内），且 0 例"变化的 key 原映射目标不是被移除实例"的违例，可以直接采用，不需要在实现阶段临时调整数值。
5. **关键 API 签名**（均已用 `javap` 核实，可直接按此签名编码，无需再查文档）：
   - `RoundRobinLoadBalancer(ObjectProvider<ServiceInstanceListSupplier>, String)` 构造器存在，`choose(Request)` 返回 `Mono<Response<ServiceInstance>>`。
   - `LoadBalancerClientFactory`（继承自 `NamedContextFactory`）有 `<T> ObjectProvider<T> getLazyProvider(String, Class<T>)`。
   - `org.springframework.cloud.client.loadbalancer.DefaultResponse(ServiceInstance)`、`EmptyResponse()` 无参构造均存在，包路径为 `org.springframework.cloud.client.loadbalancer`（不在 `reactive` 子包下）。
   - Caffeine 的 Java 包名是 `com.github.benmanes.caffeine.cache`（无连字符，区别于 Maven 坐标 `com.github.ben-manes.caffeine`），`Cache<K,V>.get(K, Function<? super K,? extends V>)` 签名存在。
   - `org.springframework.cloud.client.DefaultServiceInstance(String instanceId, String serviceId, String host, int port, boolean secure, Map<String,String> metadata)` 构造器可直接用作测试替身，不需要 mock `ServiceInstance`。
   - `gateway-loadbalancer/build.gradle` 当前**没有**显式声明 `tools.jackson.core:jackson-databind`（虽然通过其他依赖链能传递到 compile classpath，但不应依赖这种偶然的传递关系），Task 5 需要像 `gateway-ratelimit/build.gradle` 一样显式加上。
6. **`HashKeyMissingLogger` 的节流哨兵值不能用 `AtomicLong` 默认初值 0**：本计划已经把 Task 6 的实现和测试实际跑过一遍，最初按 `new AtomicLong()`（默认 0）实现时，`warnIfDue_shouldLog_onFirstCall`（受控测试时钟从 0 开始）会失败——因为 `now(0) - prev(0) = 0 < windowMillis`，首次调用被误判为"窗口内"从而漏打日志。生产环境用真实 `System.currentTimeMillis()`（epoch 毫秒，约 1.7×10¹²）不会触发这个问题，但会让测试无法用小整数时钟验证节流语义。Task 6 的实现已改为 `computeIfAbsent(serviceId, k -> new AtomicLong(Long.MIN_VALUE / 2))` 作为哨兵初值，直接按此实现，不要用默认无参 `AtomicLong()`。

## 文件结构

```
gateway-loadbalancer/
├── build.gradle                                          [改] 显式声明 jackson-databind
├── src/main/java/io/aegis/gateway/loadbalancer/
│   ├── hash/
│   │   ├── MurmurHash3.java                               [新] 包内可见的哈希函数
│   │   ├── ConsistentHashRing.java                        [新] 哈希环 + 虚拟节点 + 权重
│   │   ├── HashKeyExtractor.java                          [新] key 提取接口
│   │   └── DefaultHashKeyExtractor.java                   [新] 唯一实现（CLIENT_IP/HEADER 分支）
│   ├── loadbalance/
│   │   ├── LoadBalanceStrategy.java                       [新] 策略枚举
│   │   ├── HashKeySource.java                             [新] key 来源枚举
│   │   ├── LoadBalancePolicy.java                         [新] policy record
│   │   ├── LoadBalanceGovernanceConfig.java                [新] governance 配置段 record
│   │   ├── LoadBalancePolicyRepository.java                [新] 监听 Nacos，维护 policy 快照
│   │   ├── HashKeyMissingLogger.java                      [新] 按 serviceId 限流的 WARN 日志
│   │   └── ConsistentHashReactiveLoadBalancer.java         [新] 核心 LB 实现
│   └── config/
│       ├── AegisLoadBalancerAutoConfiguration.java         [改] 注册 LoadBalancePolicyRepository
│       └── AegisNamespaceLoadBalancerClientConfiguration.java [改] 注册一致性哈希 LB Bean
└── src/test/java/io/aegis/gateway/loadbalancer/
    ├── hash/
    │   ├── MurmurHash3Test.java                            [新]
    │   ├── ConsistentHashRingTest.java                     [新]
    │   └── DefaultHashKeyExtractorTest.java                [新]
    ├── loadbalance/
    │   ├── LoadBalancePolicyRepositoryTest.java            [新]
    │   ├── HashKeyMissingLoggerTest.java                   [新]
    │   └── ConsistentHashReactiveLoadBalancerTest.java     [新]
    └── config/
        └── AegisLoadBalancerAutoConfigurationTest.java     [改] 追加装配测试

gateway-loadbalancer/CLAUDE.md                              [改] 记录新能力
docs/rules/architecture-overview.md                          [改] gateway-loadbalancer 一行描述
```

---

## Task 1: `hash.MurmurHash3` —— 手写哈希函数

**Files:**
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/hash/MurmurHash3.java`
- Test: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/hash/MurmurHash3Test.java`

**Interfaces:**
- Produces: `static int MurmurHash3.hash(byte[] data, int seed)`（包内可见，供 Task 2 的 `ConsistentHashRing` 使用）

- [x] **Task 1 Step 1: 写失败测试**

创建 `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/hash/MurmurHash3Test.java`：

```java
package io.aegis.gateway.loadbalancer.hash;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class MurmurHash3Test {

    // 下面几组期望值已用 Python 参考实现（mmh3 库，业界标准 MurmurHash3_x86_32 实现）
    // 交叉验证过，覆盖 4 字节对齐边界的全部尾部分支（rem=0/1/2/3），不是随手编造的数字。

    @Test
    void hash_ofEmptyInput_shouldBeZero() {
        assertThat(MurmurHash3.hash(new byte[0], 0)).isZero();
    }

    @Test
    void hash_ofTwoByteInput_shouldMatchReferenceVector() {
        assertThat(MurmurHash3.hash("ab".getBytes(StandardCharsets.UTF_8), 0)).isEqualTo(-1681926305);
    }

    @Test
    void hash_ofThreeByteInput_shouldMatchReferenceVector() {
        assertThat(MurmurHash3.hash("abc".getBytes(StandardCharsets.UTF_8), 0)).isEqualTo(-1277324294);
    }

    @Test
    void hash_ofFourByteInput_shouldMatchReferenceVector() {
        assertThat(MurmurHash3.hash("test".getBytes(StandardCharsets.UTF_8), 0)).isEqualTo(-1167338989);
    }

    @Test
    void hash_ofFiveByteInput_shouldMatchReferenceVector() {
        assertThat(MurmurHash3.hash("hello".getBytes(StandardCharsets.UTF_8), 0)).isEqualTo(613153351);
    }

    @Test
    void hash_withDifferentSeed_shouldMatchReferenceVector() {
        assertThat(MurmurHash3.hash("test".getBytes(StandardCharsets.UTF_8), 42)).isEqualTo(-335093414);
    }

    @Test
    void hash_isDeterministic_forSameInputAndSeed() {
        byte[] data = "order-service-1-0".getBytes(StandardCharsets.UTF_8);

        int first = MurmurHash3.hash(data, 7);
        int second = MurmurHash3.hash(data, 7);

        assertThat(first).isEqualTo(second);
    }
}
```

- [x] **Task 1 Step 2: 运行测试确认失败**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.hash.MurmurHash3Test"`
Expected: 编译失败（`MurmurHash3` 类不存在）

- [x] **Task 1 Step 3: 实现 MurmurHash3**

创建 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/hash/MurmurHash3.java`：

```java
package io.aegis.gateway.loadbalancer.hash;

/**
 * MurmurHash3（32-bit，x86 变体）手写实现。
 * <p>
 * 只在 {@link ConsistentHashRing} 构建虚拟节点位置、计算 key 在环上的落点时使用，是包内
 * 可见的无状态纯函数工具类，不对外暴露为公共 API 契约。之所以手写而非引入依赖：编译期
 * classpath 上没有 Guava；netty-common（含 {@code io.netty.util.internal.MurmurHash3}）
 * 虽然作为 Nacos 客户端的传递依赖存在，但位于 {@code internal} 包、没有兼容性保证，不适合
 * 直接依赖（详见设计文档"背景与约束"D9）。
 * <p>
 * 算法与业界标准 MurmurHash3_x86_32 完全一致，已用参考实现交叉验证过多组固定输入的期望值
 * （见 {@link MurmurHash3Test}），覆盖 4 字节对齐边界的全部尾部分支。
 */
final class MurmurHash3 {

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private MurmurHash3() {}

    static int hash(byte[] data, int seed) {
        int h1 = seed;
        int length = data.length;
        int roundedEnd = length & 0xfffffffc; // 向下取整到 4 字节边界

        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 = (data[i] & 0xff)
                    | ((data[i + 1] & 0xff) << 8)
                    | ((data[i + 2] & 0xff) << 16)
                    | (data[i + 3] << 24);
            k1 *= C1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= C2;
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // 处理末尾不足 4 字节的部分（rem = 1/2/3），rem = 0 时该 switch 不匹配任何分支
        int k1 = 0;
        switch (length & 0x03) {
            case 3:
                k1 ^= (data[roundedEnd + 2] & 0xff) << 16;
                // fallthrough
            case 2:
                k1 ^= (data[roundedEnd + 1] & 0xff) << 8;
                // fallthrough
            case 1:
                k1 ^= (data[roundedEnd] & 0xff);
                k1 *= C1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= C2;
                h1 ^= k1;
                break;
        }

        h1 ^= length;
        h1 = fmix(h1);
        return h1;
    }

    private static int fmix(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }
}
```

- [x] **Task 1 Step 4: 运行测试确认通过**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.hash.MurmurHash3Test"`
Expected: PASS（7 个测试全部通过）

- [x] **Task 1 Step 5: Commit**

```bash
git add gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/hash/MurmurHash3.java \
        gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/hash/MurmurHash3Test.java
git commit -m "feat(loadbalancer): add hand-written MurmurHash3 x86_32 implementation"
```

---

## Task 2: `hash.ConsistentHashRing` —— 哈希环 + 虚拟节点 + 权重

**Files:**
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/hash/ConsistentHashRing.java`
- Test: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/hash/ConsistentHashRingTest.java`

**Interfaces:**
- Consumes: `MurmurHash3.hash(byte[], int)`（Task 1）
- Produces:
  - `public static ConsistentHashRing ConsistentHashRing.build(List<ServiceInstance> instances, int virtualNodesPerWeight)`
  - `public Optional<ServiceInstance> route(String key)`
  - `public static double resolveWeight(ServiceInstance instance)`（Task 7 的 `ConsistentHashReactiveLoadBalancer.buildCacheKey` 会复用这个方法，避免在两处重复解析 `"nacos.weight"` metadata）

- [x] **Task 2 Step 1: 写失败测试（虚拟节点数按权重比例分配 + 相同 key 稳定路由 + 空实例列表）**

创建 `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/hash/ConsistentHashRingTest.java`：

```java
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
```

- [x] **Task 2 Step 2: 运行测试确认失败**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.hash.ConsistentHashRingTest"`
Expected: 编译失败（`ConsistentHashRing` 类不存在）

- [x] **Task 2 Step 3: 实现 ConsistentHashRing**

创建 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/hash/ConsistentHashRing.java`：

```java
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
```

- [x] **Task 2 Step 4: 运行测试确认通过**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.hash.ConsistentHashRingTest"`
Expected: PASS（4 个测试全部通过）

- [x] **Task 2 Step 5: Commit**

```bash
git add gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/hash/ConsistentHashRing.java \
        gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/hash/ConsistentHashRingTest.java
git commit -m "feat(loadbalancer): add ConsistentHashRing with weighted virtual nodes"
```

---

## Task 3: `loadbalance` 包 —— governance policy 模型（枚举 + record）

**Files:**
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalanceStrategy.java`
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/HashKeySource.java`
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalancePolicy.java`
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalanceGovernanceConfig.java`

**Interfaces:**
- Produces:
  - `enum LoadBalanceStrategy { CONSISTENT_HASH }`
  - `enum HashKeySource { CLIENT_IP, HEADER }`
  - `record LoadBalancePolicy(String serviceId, LoadBalanceStrategy strategy, HashKeySource keySource, String keyName, Integer virtualNodesPerWeight)`，含 `public static final int DEFAULT_VIRTUAL_NODES_PER_WEIGHT = 160`
  - `record LoadBalanceGovernanceConfig(List<LoadBalancePolicy> loadBalancePolicies)`

这些是纯数据模型（枚举 + record），没有可独立测试的行为分支（`LoadBalanceGovernanceConfig` 的 null-safety 由 Task 5 的 `LoadBalancePolicyRepositoryTest` 间接覆盖：反序列化含 `loadBalancePolicies` 字段的真实 JSON 本身就验证了 record 的构造行为）。本任务不写独立测试，直接实现 + 编译通过 + commit。

- [x] **Task 3 Step 1: 创建 LoadBalanceStrategy 枚举**

创建 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalanceStrategy.java`：

```java
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
```

- [x] **Task 3 Step 2: 创建 HashKeySource 枚举**

创建 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/HashKeySource.java`：

```java
package io.aegis.gateway.loadbalancer.loadbalance;

/**
 * 一致性哈希路由 key 的来源。第一版只支持这两种，不支持 query 参数或 Cookie
 * （见设计文档 Non-Goals）。
 */
public enum HashKeySource {
    /** 取客户端 IP，经 {@code X-Forwarded-For} 请求头识别；网关前没有反向代理写入该头时无法取值，触发降级。 */
    CLIENT_IP,
    /** 取指定请求头的值，header 名由 {@link LoadBalancePolicy#keyName()} 指定，该来源下 keyName 必填。 */
    HEADER
}
```

- [x] **Task 3 Step 3: 创建 LoadBalancePolicy record**

创建 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalancePolicy.java`：

```java
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
 *                               {@link #DEFAULT_VIRTUAL_NODES_PER_WEIGHT}；提供时必须 > 0
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
}
```

- [x] **Task 3 Step 4: 创建 LoadBalanceGovernanceConfig record**

创建 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalanceGovernanceConfig.java`：

```java
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
```

- [x] **Task 3 Step 5: 编译确认通过**

Run: `./gradlew :gateway-loadbalancer:compileJava`
Expected: BUILD SUCCESSFUL

- [x] **Task 3 Step 6: Commit**

```bash
git add gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalanceStrategy.java \
        gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/HashKeySource.java \
        gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalancePolicy.java \
        gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalanceGovernanceConfig.java
git commit -m "feat(loadbalancer): add load balance governance policy models"
```

---

## Task 4: `hash.HashKeyExtractor` —— key 提取

**Files:**
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/hash/HashKeyExtractor.java`
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/hash/DefaultHashKeyExtractor.java`
- Test: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/hash/DefaultHashKeyExtractorTest.java`

**Interfaces:**
- Consumes: `LoadBalancePolicy`、`HashKeySource`（Task 3）
- Produces: `public interface HashKeyExtractor { Optional<String> extract(Request<?> request, LoadBalancePolicy policy); }`，唯一实现 `public final class DefaultHashKeyExtractor implements HashKeyExtractor`（Task 7 的 `ConsistentHashReactiveLoadBalancer` 会 `new` 这个实现）

- [ ] **Task 4 Step 1: 写失败测试**

创建 `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/hash/DefaultHashKeyExtractorTest.java`：

```java
package io.aegis.gateway.loadbalancer.hash;

import io.aegis.gateway.loadbalancer.loadbalance.HashKeySource;
import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicy;
import io.aegis.gateway.loadbalancer.loadbalance.LoadBalanceStrategy;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.loadbalancer.DefaultRequest;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestData;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultHashKeyExtractorTest {

    private final DefaultHashKeyExtractor extractor = new DefaultHashKeyExtractor();

    @Test
    void extract_shouldReturnFirstForwardedIp_whenKeySourceIsClientIp() {
        LoadBalancePolicy policy = policy(HashKeySource.CLIENT_IP, null);
        Request<RequestDataContext> request = requestWithHeader("X-Forwarded-For", "1.2.3.4, 5.6.7.8");

        Optional<String> key = extractor.extract(request, policy);

        assertThat(key).contains("1.2.3.4");
    }

    @Test
    void extract_shouldReturnEmpty_whenClientIpHeaderMissing() {
        LoadBalancePolicy policy = policy(HashKeySource.CLIENT_IP, null);
        Request<RequestDataContext> request = requestWithHeader("X-Other-Header", "irrelevant");

        assertThat(extractor.extract(request, policy)).isEmpty();
    }

    @Test
    void extract_shouldReturnHeaderValue_whenKeySourceIsHeader() {
        LoadBalancePolicy policy = policy(HashKeySource.HEADER, "X-User-Id");
        Request<RequestDataContext> request = requestWithHeader("X-User-Id", "u10086");

        Optional<String> key = extractor.extract(request, policy);

        assertThat(key).contains("u10086");
    }

    @Test
    void extract_shouldReturnEmpty_whenConfiguredHeaderMissing() {
        LoadBalancePolicy policy = policy(HashKeySource.HEADER, "X-User-Id");
        Request<RequestDataContext> request = requestWithHeader("X-Other-Header", "irrelevant");

        assertThat(extractor.extract(request, policy)).isEmpty();
    }

    @Test
    void extract_shouldReturnEmpty_whenRequestContextIsNotRequestDataContext() {
        LoadBalancePolicy policy = policy(HashKeySource.HEADER, "X-User-Id");
        Request<String> request = new DefaultRequest<>("not-a-request-data-context");

        assertThat(extractor.extract(request, policy)).isEmpty();
    }

    private static LoadBalancePolicy policy(HashKeySource keySource, String keyName) {
        return new LoadBalancePolicy("order-service", LoadBalanceStrategy.CONSISTENT_HASH, keySource, keyName, null);
    }

    private static Request<RequestDataContext> requestWithHeader(String name, String value) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders").header(name, value).build());
        RequestData requestData = new RequestData(exchange.getRequest(), exchange.getAttributes());
        return new DefaultRequest<>(new RequestDataContext(requestData, "default"));
    }
}
```

- [ ] **Task 4 Step 2: 运行测试确认失败**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.hash.DefaultHashKeyExtractorTest"`
Expected: 编译失败（`HashKeyExtractor` / `DefaultHashKeyExtractor` 不存在）

- [ ] **Task 4 Step 3: 实现 HashKeyExtractor 接口**

创建 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/hash/HashKeyExtractor.java`：

```java
package io.aegis.gateway.loadbalancer.hash;

import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicy;
import org.springframework.cloud.client.loadbalancer.Request;

import java.util.Optional;

/**
 * 从一次负载均衡请求中按 policy 配置的来源提取一致性哈希路由 key。
 * <p>
 * 取不到值（header 未携带、request 上下文类型不符等）一律返回空，不抛异常——调用方
 * （{@code ConsistentHashReactiveLoadBalancer}）据此触发降级为轮询。
 */
public interface HashKeyExtractor {
    Optional<String> extract(Request<?> request, LoadBalancePolicy policy);
}
```

- [ ] **Task 4 Step 4: 实现 DefaultHashKeyExtractor**

创建 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/hash/DefaultHashKeyExtractor.java`：

```java
package io.aegis.gateway.loadbalancer.hash;

import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicy;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestData;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.http.HttpHeaders;

import java.util.Optional;

/**
 * {@link HashKeyExtractor} 的唯一实现，内部按 {@link LoadBalancePolicy#keySource()} 分支，
 * 而不是为每种来源写一个策略类——第一版只有两种来源，分支比多态更直接。
 * <p>
 * Request 上下文解包方式与 {@code NamespaceAwareNacosServiceInstanceListSupplier.extractAttributes()}
 * 同款：只有当 {@code request.getContext()} 是 {@link RequestDataContext} 且内部
 * {@link RequestData} 非空时才能取到请求头，否则视为缺失，跨模块保持一致的请求上下文访问约定。
 * <p>
 * CLIENT_IP 来源只能读 {@code X-Forwarded-For} 请求头：当前依赖的
 * {@code spring-cloud-loadbalancer} 版本里，{@link RequestData} 不携带原始连接的远端地址
 * （没有 {@code getRemoteAddress()} 这类方法），只保留了 method/URI/headers/cookies/attributes，
 * 因此无法回退到"真实 socket 远端地址"，网关前没有反向代理写入该头时会直接判定 key 缺失。
 */
public final class DefaultHashKeyExtractor implements HashKeyExtractor {

    private static final String CLIENT_IP_HEADER = "X-Forwarded-For";

    @Override
    public Optional<String> extract(Request<?> request, LoadBalancePolicy policy) {
        HttpHeaders headers = extractHeaders(request);
        if (headers == null) {
            return Optional.empty();
        }
        return switch (policy.keySource()) {
            case CLIENT_IP -> extractClientIp(headers);
            case HEADER -> extractHeaderValue(headers, policy.keyName());
        };
    }

    private Optional<String> extractClientIp(HttpHeaders headers) {
        String forwarded = headers.getFirst(CLIENT_IP_HEADER);
        if (forwarded == null || forwarded.isBlank()) {
            return Optional.empty();
        }
        // X-Forwarded-For 可能是多级代理拼接的逗号分隔列表，第一个是最原始的客户端地址
        String first = forwarded.split(",")[0].trim();
        return first.isEmpty() ? Optional.empty() : Optional.of(first);
    }

    private Optional<String> extractHeaderValue(HttpHeaders headers, String headerName) {
        String value = headers.getFirst(headerName);
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
    }

    private HttpHeaders extractHeaders(Request<?> request) {
        if (request == null || !(request.getContext() instanceof RequestDataContext context)) {
            return null;
        }
        RequestData requestData = context.getClientRequest();
        return requestData == null ? null : requestData.getHeaders();
    }
}
```

- [ ] **Task 4 Step 5: 运行测试确认通过**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.hash.DefaultHashKeyExtractorTest"`
Expected: PASS（5 个测试全部通过）

- [ ] **Task 4 Step 6: Commit**

```bash
git add gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/hash/HashKeyExtractor.java \
        gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/hash/DefaultHashKeyExtractor.java \
        gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/hash/DefaultHashKeyExtractorTest.java
git commit -m "feat(loadbalancer): add HashKeyExtractor for CLIENT_IP/HEADER key sources"
```

---

## Task 5: `loadbalance.LoadBalancePolicyRepository` —— 监听 Nacos，维护 policy 快照

**Files:**
- Modify: `gateway-loadbalancer/build.gradle`（显式声明 `tools.jackson.core:jackson-databind`）
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalancePolicyRepository.java`
- Test: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalancePolicyRepositoryTest.java`

**Interfaces:**
- Consumes: `NacosConfigSyncService.registerGovernanceListener(Consumer<String>)`（`gateway-core`）、`LoadBalanceGovernanceConfig`/`LoadBalancePolicy`/`HashKeySource`（Task 3）
- Produces: `public LoadBalancePolicyRepository(NacosConfigSyncService syncService, ObjectMapper objectMapper)`、`public Optional<LoadBalancePolicy> findByServiceId(String serviceId)`（Task 7、Task 8 都要用）

- [ ] **Task 5 Step 1: 给 gateway-loadbalancer 显式声明 jackson-databind 依赖**

修改 `gateway-loadbalancer/build.gradle`：

```groovy
dependencies {
    implementation project(':gateway-core')
    compileOnly 'org.springframework.cloud:spring-cloud-starter-gateway-server-webflux'
    testImplementation 'org.springframework.cloud:spring-cloud-starter-gateway-server-webflux'
    implementation 'org.springframework.cloud:spring-cloud-starter-loadbalancer'
    implementation 'com.alibaba.cloud:spring-cloud-starter-alibaba-nacos-discovery'
    implementation 'com.github.ben-manes.caffeine:caffeine'
    // LoadBalancePolicyRepository 需要反序列化 governance JSON，此前该模块从未直接用过
    // Jackson，只是通过其他依赖链偶然传递到 compile classpath；显式声明避免依赖这种偶然关系
    // （镜像 gateway-ratelimit/build.gradle 的做法）。注意包名是 tools.jackson.*
    // （Spring Boot 4 的 Jackson 3），不是 com.fasterxml.jackson。
    implementation 'tools.jackson.core:jackson-databind'
}
```

- [ ] **Task 5 Step 2: 写失败测试**

创建 `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalancePolicyRepositoryTest.java`：

```java
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
```

- [ ] **Task 5 Step 3: 运行测试确认失败**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicyRepositoryTest"`
Expected: 编译失败（`LoadBalancePolicyRepository` 不存在）

- [ ] **Task 5 Step 4: 实现 LoadBalancePolicyRepository**

创建 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalancePolicyRepository.java`：

```java
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
            if (policy.virtualNodesPerWeight() != null && policy.virtualNodesPerWeight() <= 0) {
                throw new IllegalArgumentException("virtualNodesPerWeight must be positive: " + policy.serviceId());
            }
        }
    }

    private static void requireText(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
    }
}
```

- [ ] **Task 5 Step 5: 运行测试确认通过**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicyRepositoryTest"`
Expected: PASS（6 个测试全部通过）

- [ ] **Task 5 Step 6: Commit**

```bash
git add gateway-loadbalancer/build.gradle \
        gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalancePolicyRepository.java \
        gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/loadbalance/LoadBalancePolicyRepositoryTest.java
git commit -m "feat(loadbalancer): add LoadBalancePolicyRepository backed by Nacos governance"
```

---

## Task 6: `loadbalance.HashKeyMissingLogger` —— 按 serviceId 限流的 WARN 日志

**Files:**
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/HashKeyMissingLogger.java`
- Test: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/loadbalance/HashKeyMissingLoggerTest.java`

**Interfaces:**
- Consumes: `LoadBalancePolicy`（Task 3）
- Produces: 包内可见 `boolean HashKeyMissingLogger.warnIfDue(String serviceId, LoadBalancePolicy policy)`（返回值供测试验证节流语义；Task 7 的调用方忽略返回值即可，用法与设计文档一致）

> 设计文档给的签名是 `void warnIfDue(...)`。这里改成 `boolean`（是否实际打印了日志）纯粹是为了可测试性，镜像 `RedissonClientManager.retryLater()` "返回 boolean 便于测试验证冷却语义"的既有写法（见 `gateway-ratelimit/src/main/java/io/aegis/gateway/ratelimit/core/RedissonClientManager.java:78-80`）。调用方完全不需要感知这个返回值，行为不变。

- [ ] **Task 6 Step 1: 写失败测试**

创建 `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/loadbalance/HashKeyMissingLoggerTest.java`：

```java
package io.aegis.gateway.loadbalancer.loadbalance;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class HashKeyMissingLoggerTest {

    private final LoadBalancePolicy policy =
            new LoadBalancePolicy("order-service", LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", null);

    @Test
    void warnIfDue_shouldLog_onFirstCall() {
        AtomicLong clock = new AtomicLong(0);
        HashKeyMissingLogger logger = new HashKeyMissingLogger(1000, clock::get);

        assertThat(logger.warnIfDue("order-service", policy)).isTrue();
    }

    @Test
    void warnIfDue_shouldNotLog_whenCalledAgainWithinWindow() {
        AtomicLong clock = new AtomicLong(0);
        HashKeyMissingLogger logger = new HashKeyMissingLogger(1000, clock::get);
        logger.warnIfDue("order-service", policy);

        clock.set(500); // 未超过 1000ms 窗口

        assertThat(logger.warnIfDue("order-service", policy)).isFalse();
    }

    @Test
    void warnIfDue_shouldLogAgain_afterWindowElapses() {
        AtomicLong clock = new AtomicLong(0);
        HashKeyMissingLogger logger = new HashKeyMissingLogger(1000, clock::get);
        logger.warnIfDue("order-service", policy);

        clock.set(1000); // 恰好到达窗口边界

        assertThat(logger.warnIfDue("order-service", policy)).isTrue();
    }

    @Test
    void warnIfDue_shouldThrottleIndependently_perServiceId() {
        AtomicLong clock = new AtomicLong(0);
        HashKeyMissingLogger logger = new HashKeyMissingLogger(1000, clock::get);
        logger.warnIfDue("order-service", policy);

        // 不同 serviceId 的节流状态互不影响，即使 order-service 刚打印过
        assertThat(logger.warnIfDue("user-service", policy)).isTrue();
    }
}
```

- [ ] **Task 6 Step 2: 运行测试确认失败**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.loadbalance.HashKeyMissingLoggerTest"`
Expected: 编译失败（`HashKeyMissingLogger` 不存在）

- [ ] **Task 6 Step 3: 实现 HashKeyMissingLogger**

创建 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/HashKeyMissingLogger.java`：

```java
package io.aegis.gateway.loadbalancer.loadbalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * 一致性哈希 key 缺失时按 serviceId 限流打印 WARN 日志，避免配置错误
 * （如 header 名写错导致所有请求都缺 key）时刷屏拖累网关。
 * <p>
 * 每个 serviceId 维护一个"上次打印时间"的原子时间戳，距上次打印超过 {@code windowMillis}
 * 才允许再打印一条；{@code compareAndSet} 保证并发请求同时触发时只有一个线程真正打印，
 * 其余线程直接跳过，不需要额外加锁。
 */
final class HashKeyMissingLogger {

    private static final Logger log = LoggerFactory.getLogger(HashKeyMissingLogger.class);

    private static final long DEFAULT_WINDOW_MILLIS = 30_000;

    private final ConcurrentHashMap<String, AtomicLong> lastLoggedAt = new ConcurrentHashMap<>();
    private final long windowMillis;
    private final LongSupplier clock;

    HashKeyMissingLogger() {
        this(DEFAULT_WINDOW_MILLIS, System::currentTimeMillis);
    }

    /** 测试入口：注入更短的窗口和可控时钟，避免真实 sleep 等待。 */
    HashKeyMissingLogger(long windowMillis, LongSupplier clock) {
        this.windowMillis = windowMillis;
        this.clock = clock;
    }

    /**
     * @return 本次调用是否实际打印了日志（供测试验证限流窗口语义，调用方无需关心返回值）
     */
    boolean warnIfDue(String serviceId, LoadBalancePolicy policy) {
        long now = clock.getAsLong();
        // 哨兵值必须保证首次调用一定判定为"已过窗口"：AtomicLong 默认初值 0 在真实时钟
        // （epoch 毫秒，约 1.7e12）下天然满足这一点，但受控测试时钟从 0 开始时会和它撞在
        // 一起，导致第一次调用被误判为"窗口内"而漏打日志。用 Long.MIN_VALUE / 2 兜底，
        // 保证无论时钟从多小的值起算，首次调用的 now - prev 都远大于 windowMillis，
        // 同时足够小以避免 now - prev 在极端情况下溢出。
        AtomicLong last = lastLoggedAt.computeIfAbsent(serviceId, k -> new AtomicLong(Long.MIN_VALUE / 2));
        long prev = last.get();
        if (now - prev >= windowMillis && last.compareAndSet(prev, now)) {
            log.warn("Consistent hash key missing, degraded to round-robin. serviceId={}, keySource={}, keyName={}",
                    serviceId, policy.keySource(), policy.keyName());
            return true;
        }
        return false;
    }
}
```

- [ ] **Task 6 Step 4: 运行测试确认通过**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.loadbalance.HashKeyMissingLoggerTest"`
Expected: PASS（4 个测试全部通过）

- [ ] **Task 6 Step 5: Commit**

```bash
git add gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/HashKeyMissingLogger.java \
        gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/loadbalance/HashKeyMissingLoggerTest.java
git commit -m "feat(loadbalancer): add throttled WARN logging for missing hash keys"
```

---

## Task 7: `loadbalance.ConsistentHashReactiveLoadBalancer` —— 核心 LB 实现

**Files:**
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/ConsistentHashReactiveLoadBalancer.java`
- Test: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/loadbalance/ConsistentHashReactiveLoadBalancerTest.java`

**Interfaces:**
- Consumes: `ConsistentHashRing`/`ConsistentHashRing.resolveWeight()`（Task 2）、`HashKeyExtractor`/`DefaultHashKeyExtractor`（Task 4）、`LoadBalancePolicyRepository`（Task 5）、`HashKeyMissingLogger`（Task 6）
- Produces: `public ConsistentHashReactiveLoadBalancer(ObjectProvider<ServiceInstanceListSupplier> supplierProvider, String serviceId, LoadBalancePolicyRepository policyRepository)`，实现 `ReactiveLoadBalancer<ServiceInstance>`（Task 8 的 Bean 装配会 `new` 这个类）

- [ ] **Task 7 Step 1: 写失败测试（无 policy → 委托轮询；policy+key → 稳定路由）**

创建 `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/loadbalance/ConsistentHashReactiveLoadBalancerTest.java`（先写前两个场景）：

```java
package io.aegis.gateway.loadbalancer.loadbalance;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.aegis.gateway.loadbalancer.hash.DefaultHashKeyExtractor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.DefaultRequest;
import org.springframework.cloud.client.loadbalancer.DefaultResponse;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestData;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.loadbalancer.core.RoundRobinLoadBalancer;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ConsistentHashReactiveLoadBalancerTest {

    private static final String SERVICE_ID = "order-service";

    @SuppressWarnings("unchecked")
    private final ObjectProvider<ServiceInstanceListSupplier> supplierProvider = mock(ObjectProvider.class);
    private final LoadBalancePolicyRepository policyRepository = mock(LoadBalancePolicyRepository.class);
    private final RoundRobinLoadBalancer delegate = mock(RoundRobinLoadBalancer.class);

    @Test
    void choose_shouldDelegateToRoundRobin_whenNoPolicyConfigured() {
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.empty());
        ServiceInstance instance = instance("10.0.0.1", 8080, null);
        when(delegate.choose(any())).thenReturn(Mono.just(new DefaultResponse(instance)));
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, new HashKeyMissingLogger());
        Request<RequestDataContext> request = requestWithHeader("X-User-Id", "u1");

        StepVerifier.create(loadBalancer.choose(request))
                .assertNext(response -> assertThat(response.getServer()).isEqualTo(instance))
                .verifyComplete();

        verify(delegate).choose(request);
        verifyNoInteractions(supplierProvider);
    }

    @Test
    void choose_shouldRouteToSameInstance_forSameKey_whenPolicyConfigured() {
        LoadBalancePolicy policy = new LoadBalancePolicy(
                SERVICE_ID, LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", 160);
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.of(policy));
        List<ServiceInstance> instances = List.of(
                instance("10.0.0.1", 8080, null),
                instance("10.0.0.2", 8080, null),
                instance("10.0.0.3", 8080, null));
        ServiceInstanceListSupplier supplier = mock(ServiceInstanceListSupplier.class);
        when(supplier.get(any())).thenReturn(Flux.just(instances));
        when(supplierProvider.getIfAvailable()).thenReturn(supplier);
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, new HashKeyMissingLogger());
        Request<RequestDataContext> request = requestWithHeader("X-User-Id", "u10086");

        ServiceInstance first = choose(loadBalancer, request);
        ServiceInstance second = choose(loadBalancer, request);

        assertThat(first).isEqualTo(second);
        assertThat(instances).contains(first);
        verifyNoInteractions(delegate);
    }

    @Test
    void choose_shouldDegradeToRoundRobin_whenKeyMissing() {
        LoadBalancePolicy policy = new LoadBalancePolicy(
                SERVICE_ID, LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", 160);
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.of(policy));
        ServiceInstanceListSupplier supplier = mock(ServiceInstanceListSupplier.class);
        when(supplier.get(any())).thenReturn(Flux.just(List.of(instance("10.0.0.1", 8080, null))));
        when(supplierProvider.getIfAvailable()).thenReturn(supplier);
        ServiceInstance fallback = instance("10.0.0.9", 8080, null);
        when(delegate.choose(any())).thenReturn(Mono.just(new DefaultResponse(fallback)));
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, new HashKeyMissingLogger());
        // 请求没有携带 policy 要求的 X-User-Id header
        Request<RequestDataContext> request = requestWithHeader("X-Other-Header", "irrelevant");

        StepVerifier.create(loadBalancer.choose(request))
                .assertNext(response -> assertThat(response.getServer()).isEqualTo(fallback))
                .verifyComplete();

        verify(delegate).choose(request);
    }

    @Test
    void choose_shouldOnlyCallMissingLoggerOncePerCall_andDelegateThrottlingToIt() {
        LoadBalancePolicy policy = new LoadBalancePolicy(
                SERVICE_ID, LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", 160);
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.of(policy));
        ServiceInstanceListSupplier supplier = mock(ServiceInstanceListSupplier.class);
        when(supplier.get(any())).thenReturn(Flux.just(List.of(instance("10.0.0.1", 8080, null))));
        when(supplierProvider.getIfAvailable()).thenReturn(supplier);
        when(delegate.choose(any())).thenReturn(Mono.just(new DefaultResponse(instance("10.0.0.9", 8080, null))));
        HashKeyMissingLogger missingLogger = mock(HashKeyMissingLogger.class);
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, missingLogger);
        Request<RequestDataContext> request = requestWithHeader("X-Other-Header", "irrelevant");

        loadBalancer.choose(request).block();
        loadBalancer.choose(request).block();

        // 节流窗口本身的语义在 HashKeyMissingLoggerTest 里验证；这里只验证每次 key 缺失
        // 都正确调用了 warnIfDue，把"是否真的打印"这个决定完全交给该协作者
        verify(missingLogger, times(2)).warnIfDue(SERVICE_ID, policy);
    }

    @Test
    void choose_shouldReturnEmptyResponse_whenCandidateInstanceListIsEmpty() {
        LoadBalancePolicy policy = new LoadBalancePolicy(
                SERVICE_ID, LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", 160);
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.of(policy));
        ServiceInstanceListSupplier supplier = mock(ServiceInstanceListSupplier.class);
        when(supplier.get(any())).thenReturn(Flux.just(List.of()));
        when(supplierProvider.getIfAvailable()).thenReturn(supplier);
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, new HashKeyMissingLogger());
        Request<RequestDataContext> request = requestWithHeader("X-User-Id", "u1");

        StepVerifier.create(loadBalancer.choose(request))
                .assertNext(response -> assertThat(response.hasServer()).isFalse())
                .verifyComplete();

        verifyNoInteractions(delegate);
    }

    @Test
    void choose_shouldFallBackToRoundRobin_whenSupplierUnavailable() {
        LoadBalancePolicy policy = new LoadBalancePolicy(
                SERVICE_ID, LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", 160);
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.of(policy));
        when(supplierProvider.getIfAvailable()).thenReturn(null);
        ServiceInstance fallback = instance("10.0.0.9", 8080, null);
        when(delegate.choose(any())).thenReturn(Mono.just(new DefaultResponse(fallback)));
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, new HashKeyMissingLogger());
        Request<RequestDataContext> request = requestWithHeader("X-User-Id", "u1");

        StepVerifier.create(loadBalancer.choose(request))
                .assertNext(response -> assertThat(response.getServer()).isEqualTo(fallback))
                .verifyComplete();
    }

    @Test
    void choose_shouldOnlyRerouteKeysThatHitRemovedInstance_whenCandidateListShrinks() {
        LoadBalancePolicy policy = new LoadBalancePolicy(
                SERVICE_ID, LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", 160);
        when(policyRepository.findByServiceId(SERVICE_ID)).thenReturn(Optional.of(policy));
        List<ServiceInstance> fiveInstances = List.of(
                instance("10.0.0.1", 8080, null), instance("10.0.0.2", 8080, null),
                instance("10.0.0.3", 8080, null), instance("10.0.0.4", 8080, null),
                instance("10.0.0.5", 8080, null));
        ServiceInstance removed = fiveInstances.get(2);
        List<ServiceInstance> fourInstances = fiveInstances.stream().filter(i -> !i.equals(removed)).toList();
        ServiceInstanceListSupplier supplier = mock(ServiceInstanceListSupplier.class);
        when(supplierProvider.getIfAvailable()).thenReturn(supplier);
        ConsistentHashReactiveLoadBalancer loadBalancer = newLoadBalancer(delegate, new HashKeyMissingLogger());

        when(supplier.get(any())).thenReturn(Flux.just(fiveInstances));
        for (int i = 0; i < 200; i++) {
            Request<RequestDataContext> request = requestWithHeader("X-User-Id", "user-" + i);
            ServiceInstance before = choose(loadBalancer, request);

            when(supplier.get(any())).thenReturn(Flux.just(fourInstances));
            ServiceInstance after = choose(loadBalancer, request);
            when(supplier.get(any())).thenReturn(Flux.just(fiveInstances));

            if (!before.equals(after)) {
                assertThat(before).isEqualTo(removed);
                assertThat(after).isNotEqualTo(removed);
            }
        }
    }

    private ConsistentHashReactiveLoadBalancer newLoadBalancer(RoundRobinLoadBalancer delegate,
                                                                HashKeyMissingLogger missingLogger) {
        return new ConsistentHashReactiveLoadBalancer(supplierProvider, SERVICE_ID, policyRepository,
                delegate, new DefaultHashKeyExtractor(), missingLogger,
                Caffeine.newBuilder().maximumSize(2).build());
    }

    private static ServiceInstance choose(ConsistentHashReactiveLoadBalancer loadBalancer, Request<?> request) {
        Response<ServiceInstance> response = loadBalancer.choose(request).block();
        return response.getServer();
    }

    private static ServiceInstance instance(String host, int port, String weightMetadata) {
        Map<String, String> metadata = weightMetadata == null ? Map.of() : Map.of("nacos.weight", weightMetadata);
        return new DefaultServiceInstance("instance-" + host, SERVICE_ID, host, port, false, metadata);
    }

    private static Request<RequestDataContext> requestWithHeader(String name, String value) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders").header(name, value).build());
        RequestData requestData = new RequestData(exchange.getRequest(), exchange.getAttributes());
        return new DefaultRequest<>(new RequestDataContext(requestData, "default"));
    }
}
```

- [ ] **Task 7 Step 2: 运行测试确认失败**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.loadbalance.ConsistentHashReactiveLoadBalancerTest"`
Expected: 编译失败（`ConsistentHashReactiveLoadBalancer` 不存在，且缺少测试构造器）

- [ ] **Task 7 Step 3: 实现 ConsistentHashReactiveLoadBalancer**

创建 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/ConsistentHashReactiveLoadBalancer.java`：

```java
package io.aegis.gateway.loadbalancer.loadbalance;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.aegis.gateway.loadbalancer.hash.ConsistentHashRing;
import io.aegis.gateway.loadbalancer.hash.DefaultHashKeyExtractor;
import io.aegis.gateway.loadbalancer.hash.HashKeyExtractor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.DefaultResponse;
import org.springframework.cloud.client.loadbalancer.EmptyResponse;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.client.loadbalancer.reactive.ReactiveLoadBalancer;
import org.springframework.cloud.loadbalancer.core.RoundRobinLoadBalancer;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 按 serviceId 装配的一致性哈希 {@link ReactiveLoadBalancer}。
 * <p>
 * 内部持有一个 {@link RoundRobinLoadBalancer}（构造方式与 Spring Cloud LoadBalancer 自身
 * 创建默认实例完全一致）作为降级委托：serviceId 未配置一致性哈希 policy、或请求提取不到
 * 哈希 key 时，都直接委托给它，不额外引入分支逻辑，"未配置 policy" 和 "key 缺失降级"
 * 两条需求由同一个委托对象满足。
 * <p>
 * 环用 Caffeine {@code Cache<String, ConsistentHashRing>}（{@code maximumSize(2)}）
 * 按"实例集合 + 权重 + 虚拟节点数"拼成的字符串做缓存 key：只需要保留"当前"和"上一个"两份
 * 即可覆盖切换瞬间的并发读，{@code get(key, loader)} 语义天然线程安全（命中直接返回、
 * 未命中调用 loader 构建），不需要手写 {@code AtomicReference} + 签名比较 + 原子替换的
 * 样板代码。
 * <p>
 * Nacos 实例查询本身已经在 {@code NamespaceAwareNacosServiceInstanceListSupplier} 内部
 * 跑在 {@code boundedElastic} 上，这一层不需要额外调度。
 */
public class ConsistentHashReactiveLoadBalancer implements ReactiveLoadBalancer<ServiceInstance> {

    private final ObjectProvider<ServiceInstanceListSupplier> supplierProvider;
    private final String serviceId;
    private final LoadBalancePolicyRepository policyRepository;
    private final RoundRobinLoadBalancer delegate;
    private final HashKeyExtractor keyExtractor;
    private final HashKeyMissingLogger missingLogger;
    private final Cache<String, ConsistentHashRing> ringCache;

    public ConsistentHashReactiveLoadBalancer(ObjectProvider<ServiceInstanceListSupplier> supplierProvider,
                                              String serviceId,
                                              LoadBalancePolicyRepository policyRepository) {
        this(supplierProvider, serviceId, policyRepository,
                new RoundRobinLoadBalancer(supplierProvider, serviceId),
                new DefaultHashKeyExtractor(),
                new HashKeyMissingLogger(),
                Caffeine.newBuilder().<String, ConsistentHashRing>maximumSize(2).build());
    }

    /** 测试入口：注入可控的 delegate / keyExtractor / missingLogger / ringCache。 */
    ConsistentHashReactiveLoadBalancer(ObjectProvider<ServiceInstanceListSupplier> supplierProvider,
                                       String serviceId,
                                       LoadBalancePolicyRepository policyRepository,
                                       RoundRobinLoadBalancer delegate,
                                       HashKeyExtractor keyExtractor,
                                       HashKeyMissingLogger missingLogger,
                                       Cache<String, ConsistentHashRing> ringCache) {
        this.supplierProvider = supplierProvider;
        this.serviceId = serviceId;
        this.policyRepository = policyRepository;
        this.delegate = delegate;
        this.keyExtractor = keyExtractor;
        this.missingLogger = missingLogger;
        this.ringCache = ringCache;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Mono<Response<ServiceInstance>> choose(Request request) {
        LoadBalancePolicy policy = policyRepository.findByServiceId(serviceId).orElse(null);
        if (policy == null) {
            return delegate.choose(request);
        }
        ServiceInstanceListSupplier supplier = supplierProvider.getIfAvailable();
        if (supplier == null) {
            // 理论上不会发生：同一 serviceId 的 LB 子容器总会注册一个 Supplier bean；
            // 仍然按 fail-open 惯例兜底，避免防御性判断反而制造一个未覆盖的 NPE 分支
            return delegate.choose(request);
        }
        return supplier.get(request).next().flatMap(instances -> {
            Optional<String> key = keyExtractor.extract(request, policy);
            if (key.isEmpty()) {
                missingLogger.warnIfDue(serviceId, policy);
                return delegate.choose(request);
            }
            String cacheKey = buildCacheKey(instances, policy);
            ConsistentHashRing ring = ringCache.get(cacheKey,
                    k -> ConsistentHashRing.build(instances, resolveVirtualNodes(policy)));
            return Mono.just(ring.route(key.get())
                    .<Response<ServiceInstance>>map(DefaultResponse::new)
                    .orElseGet(EmptyResponse::new));
        });
    }

    private static int resolveVirtualNodes(LoadBalancePolicy policy) {
        Integer configured = policy.virtualNodesPerWeight();
        return configured != null ? configured : LoadBalancePolicy.DEFAULT_VIRTUAL_NODES_PER_WEIGHT;
    }

    // 用精确字符串（排序后的 "host:port:weight" 拼接串 + "|vn=" + 虚拟节点数）做缓存 key，
    // 不做哈希摘要——不相等就是不相等，没有碰撞概率问题；virtualNodesPerWeight 参与 key
    // 计算，保证该参数单独变化时也能触发重建。
    private static String buildCacheKey(List<ServiceInstance> instances, LoadBalancePolicy policy) {
        String instancesPart = instances.stream()
                .map(instance -> instance.getHost() + ":" + instance.getPort() + ":" + ConsistentHashRing.resolveWeight(instance))
                .sorted()
                .collect(Collectors.joining(","));
        return instancesPart + "|vn=" + resolveVirtualNodes(policy);
    }
}
```

- [ ] **Task 7 Step 4: 运行测试确认通过**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.loadbalance.ConsistentHashReactiveLoadBalancerTest"`
Expected: PASS（7 个测试全部通过）

- [ ] **Task 7 Step 5: Commit**

```bash
git add gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/loadbalance/ConsistentHashReactiveLoadBalancer.java \
        gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/loadbalance/ConsistentHashReactiveLoadBalancerTest.java
git commit -m "feat(loadbalancer): add ConsistentHashReactiveLoadBalancer"
```

---

## Task 8: Bean 装配

**Files:**
- Modify: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfiguration.java`
- Modify: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisNamespaceLoadBalancerClientConfiguration.java`
- Modify: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfigurationTest.java`

**Interfaces:**
- Consumes: `LoadBalancePolicyRepository`（Task 5）、`ConsistentHashReactiveLoadBalancer`（Task 7）、`NacosConfigSyncService`（`gateway-core`）
- Produces: `AegisLoadBalancerAutoConfiguration` 注册全局唯一的 `LoadBalancePolicyRepository` bean；`AegisNamespaceLoadBalancerClientConfiguration` 为每个 serviceId 注册 `ConsistentHashReactiveLoadBalancer` 作为 `ReactiveLoadBalancer<ServiceInstance>`

- [ ] **Task 8 Step 1: 写失败测试**

在现有 `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfigurationTest.java` 中追加以下内容（保留原有类和测试方法不动）：

在文件顶部现有 import 块中新增：

```java
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.loadbalancer.loadbalance.ConsistentHashReactiveLoadBalancer;
import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicyRepository;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.reactive.ReactiveLoadBalancer;
import org.springframework.cloud.loadbalancer.support.LoadBalancerClientFactory;
import tools.jackson.databind.ObjectMapper;
```

（`LoadBalancerClientFactory` 已在原文件中以 `org.springframework.cloud.loadbalancer.support.LoadBalancerClientFactory` 导入，不要重复添加）

在类体内追加以下测试方法：

```java
    @Test
    void autoConfiguration_shouldRegisterLoadBalancePolicyRepository_whenNacosConfigSyncServiceBeanPresent() {
        contextRunner
                .withBean(NacosConfigSyncService.class, () -> mock(NacosConfigSyncService.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .run(context -> assertThat(context).hasSingleBean(LoadBalancePolicyRepository.class));
    }

    @Test
    void autoConfiguration_shouldNotRegisterLoadBalancePolicyRepository_whenNacosConfigSyncServiceBeanMissing() {
        contextRunner
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .run(context -> assertThat(context).doesNotHaveBean(LoadBalancePolicyRepository.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void clientConfiguration_shouldCreateConsistentHashLoadBalancerBean() {
        MockEnvironment environment = new MockEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("loadbalancer-client", Map.of(
                LoadBalancerClientFactory.PROPERTY_NAME, "order-service")));
        LoadBalancerClientFactory clientFactory = mock(LoadBalancerClientFactory.class);
        ObjectProvider<ServiceInstanceListSupplier> supplierProvider = mock(ObjectProvider.class);
        when(clientFactory.getLazyProvider("order-service", ServiceInstanceListSupplier.class))
                .thenReturn(supplierProvider);
        LoadBalancePolicyRepository policyRepository = mock(LoadBalancePolicyRepository.class);
        AegisNamespaceLoadBalancerClientConfiguration configuration =
                new AegisNamespaceLoadBalancerClientConfiguration();

        ReactiveLoadBalancer<ServiceInstance> loadBalancer = configuration.aegisConsistentHashLoadBalancer(
                environment, clientFactory, policyRepository);

        assertThat(loadBalancer).isInstanceOf(ConsistentHashReactiveLoadBalancer.class);
    }
```

- [ ] **Task 8 Step 2: 运行测试确认失败**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.config.AegisLoadBalancerAutoConfigurationTest"`
Expected: 编译失败（`LoadBalancePolicyRepository` bean 方法、`aegisConsistentHashLoadBalancer` 方法均不存在）

- [ ] **Task 8 Step 3: 修改 AegisLoadBalancerAutoConfiguration**

将 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfiguration.java` 整体替换为：

```java
package io.aegis.gateway.loadbalancer.config;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.cloud.nacos.loadbalancer.LoadBalancerNacosAutoConfiguration;
import com.alibaba.nacos.api.naming.NamingService;
import io.aegis.gateway.core.config.AegisCoreAutoConfiguration;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.loadbalancer.discovery.NacosNamingServiceRegistry;
import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicyRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.loadbalancer.annotation.LoadBalancerClients;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.ObjectMapper;

/**
 * gateway-loadbalancer 模块的自动配置入口。
 * <p>
 * 注册 {@link NacosNamingServiceRegistry} Bean，并通过 {@code @LoadBalancerClients} 将
 * {@link AegisNamespaceLoadBalancerClientConfiguration} 设为所有 LoadBalancer 客户端的
 * 默认配置，使命名空间感知的 Supplier 和一致性哈希策略生效。
 * <p>
 * 同时注册全局唯一的 {@link LoadBalancePolicyRepository}：按 serviceId 装配的每个
 * `ConsistentHashReactiveLoadBalancer` 子容器都从 parent 容器共享读取这一份 policy 快照，
 * 不需要每个 serviceId 各自监听一遍 Nacos governance 配置。该 Bean 独立以
 * {@code @ConditionalOnBean(NacosConfigSyncService.class)} 限定条件（而不是把整个自动配置类
 * 挂在这个条件上），避免影响本模块其他 Bean（如 {@code NacosNamingServiceRegistry}）在
 * 单独测试场景下的既有装配行为。
 * <p>
 * 在 {@link LoadBalancerNacosAutoConfiguration} 之后运行，确保 Nacos 相关 Bean 已就绪；
 * 在 {@link AegisCoreAutoConfiguration} 之后运行，确保 {@link NacosConfigSyncService}
 * 已经注册，{@code @ConditionalOnBean(NacosConfigSyncService.class)} 才能可靠生效。
 */
@AutoConfiguration(after = { LoadBalancerNacosAutoConfiguration.class, AegisCoreAutoConfiguration.class })
@ConditionalOnClass({ NamingService.class, ServiceInstanceListSupplier.class })
@ConditionalOnBean(NacosDiscoveryProperties.class)
@LoadBalancerClients(defaultConfiguration = AegisNamespaceLoadBalancerClientConfiguration.class)
public class AegisLoadBalancerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public NacosNamingServiceRegistry nacosNamingServiceRegistry(NacosDiscoveryProperties discoveryProperties) {
        return new NacosNamingServiceRegistry(discoveryProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(NacosConfigSyncService.class)
    public LoadBalancePolicyRepository loadBalancePolicyRepository(NacosConfigSyncService syncService,
                                                                    ObjectMapper objectMapper) {
        return new LoadBalancePolicyRepository(syncService, objectMapper);
    }
}
```

- [ ] **Task 8 Step 4: 修改 AegisNamespaceLoadBalancerClientConfiguration**

将 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisNamespaceLoadBalancerClientConfiguration.java` 整体替换为：

```java
package io.aegis.gateway.loadbalancer.config;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import io.aegis.gateway.loadbalancer.discovery.NamespaceAwareNacosServiceInstanceListSupplier;
import io.aegis.gateway.loadbalancer.discovery.NacosNamingServiceRegistry;
import io.aegis.gateway.loadbalancer.loadbalance.ConsistentHashReactiveLoadBalancer;
import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicyRepository;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.reactive.ReactiveLoadBalancer;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.cloud.loadbalancer.support.LoadBalancerClientFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;

/**
 * 每个 LoadBalancer 客户端的配置类，为对应 serviceId 创建
 * {@link NamespaceAwareNacosServiceInstanceListSupplier} 和 {@link ConsistentHashReactiveLoadBalancer}。
 * <p>
 * 由 {@link AegisLoadBalancerAutoConfiguration} 通过 {@code @LoadBalancerClients(defaultConfiguration)}
 * 注册为全局默认配置，为所有服务自动应用命名空间感知的实例发现逻辑和一致性哈希策略
 * （是否真正启用一致性哈希由 {@link LoadBalancePolicyRepository} 里该 serviceId 是否存在
 * policy 决定，未配置时 {@link ConsistentHashReactiveLoadBalancer} 内部委托给标准轮询，
 * 对外行为与此前完全一致）。
 */
@Configuration(proxyBeanMethods = false)
public class AegisNamespaceLoadBalancerClientConfiguration {

    @Bean
    @ConditionalOnMissingBean(ServiceInstanceListSupplier.class)
    public ServiceInstanceListSupplier aegisServiceInstanceListSupplier(Environment environment,
                                                                       NacosDiscoveryProperties discoveryProperties,
                                                                       NacosNamingServiceRegistry namingServiceRegistry) {
        String serviceId = environment.getProperty(LoadBalancerClientFactory.PROPERTY_NAME);
        Assert.hasText(serviceId, "'serviceId' must not be empty");
        return new NamespaceAwareNacosServiceInstanceListSupplier(serviceId, discoveryProperties, namingServiceRegistry);
    }

    @Bean
    @ConditionalOnMissingBean(ReactiveLoadBalancer.class)
    public ReactiveLoadBalancer<ServiceInstance> aegisConsistentHashLoadBalancer(
            Environment environment,
            LoadBalancerClientFactory clientFactory,
            LoadBalancePolicyRepository policyRepository) {
        String serviceId = environment.getProperty(LoadBalancerClientFactory.PROPERTY_NAME);
        Assert.hasText(serviceId, "'serviceId' must not be empty");
        ObjectProvider<ServiceInstanceListSupplier> supplierProvider =
                clientFactory.getLazyProvider(serviceId, ServiceInstanceListSupplier.class);
        return new ConsistentHashReactiveLoadBalancer(supplierProvider, serviceId, policyRepository);
    }
}
```

- [ ] **Task 8 Step 5: 运行测试确认通过**

Run: `./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.config.AegisLoadBalancerAutoConfigurationTest"`
Expected: PASS（全部测试通过，含原有 2 个 + 新增 3 个）

- [ ] **Task 8 Step 6: 运行整个模块测试确认无回归**

Run: `./gradlew :gateway-loadbalancer:test`
Expected: BUILD SUCCESSFUL，全部测试通过

- [ ] **Task 8 Step 7: Commit**

```bash
git add gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfiguration.java \
        gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisNamespaceLoadBalancerClientConfiguration.java \
        gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfigurationTest.java
git commit -m "feat(loadbalancer): wire ConsistentHashReactiveLoadBalancer into bean assembly"
```

---

## Task 9: 文档同步

**Files:**
- Modify: `gateway-loadbalancer/CLAUDE.md`
- Modify: `docs/rules/architecture-overview.md`

**Interfaces:**
- 无代码接口，纯文档任务

- [ ] **Task 9 Step 1: 更新 gateway-loadbalancer/CLAUDE.md**

在 `gateway-loadbalancer/CLAUDE.md` 的"关键类"表格后、"命名空间感知发现机制"章节前，插入一个新的二级章节（保留文件其余内容不变）：

```markdown
## 一致性哈希负载均衡

按 serviceId 可选启用，配置来自 `aegis-governance.json` 的 `loadBalancePolicies` 节点，通过 `LoadBalancePolicyRepository`（监听 `NacosConfigSyncService.registerGovernanceListener`）热更新。未配置 policy 的 serviceId 行为不变，仍是 SCG 默认 `RoundRobinLoadBalancer`。

配置 schema（`aegis-governance.json` 片段）：

```json
{
  "loadBalancePolicies": [
    {
      "serviceId": "order-service",
      "strategy": "CONSISTENT_HASH",
      "keySource": "HEADER",
      "keyName": "X-User-Id",
      "virtualNodesPerWeight": 160
    }
  ]
}
```

- `keySource`：`CLIENT_IP`（读 `X-Forwarded-For` 请求头第一个值；当前依赖的 `spring-cloud-loadbalancer` 版本下 `RequestData` 不携带原始连接远端地址，只能走这个头，没有反向代理写入时会判定 key 缺失）或 `HEADER`（读 `keyName` 指定的请求头，此时 `keyName` 必填）。
- `virtualNodesPerWeight`：缺省 160（`LoadBalancePolicy.DEFAULT_VIRTUAL_NODES_PER_WEIGHT`），每个实例的虚拟节点数 = 该值 × 实例权重（权重来自 `ServiceInstance.getMetadata().get("nacos.weight")`，缺失或非法时按 1.0 处理）。

| 类（`hash` 包） | 作用 |
|---|---|
| `hash/MurmurHash3` | 包内可见的哈希函数（32-bit x86 变体），供 `ConsistentHashRing` 计算虚拟节点/key 在环上的位置 |
| `hash/ConsistentHashRing` | 哈希环：`TreeMap<Long, ServiceInstance>` + 虚拟节点 + 权重，`build()`/`route()` 两个静态/实例方法 |
| `hash/HashKeyExtractor` + `hash/DefaultHashKeyExtractor` | 按 policy 的 `keySource` 从请求提取哈希 key，取不到返回空（不抛异常） |

| 类（`loadbalance` 包） | 作用 |
|---|---|
| `loadbalance/LoadBalancePolicy` / `LoadBalanceGovernanceConfig` | governance policy 模型（record），`LoadBalanceStrategy`/`HashKeySource` 是配套枚举 |
| `loadbalance/LoadBalancePolicyRepository` | 监听 Nacos governance，维护按 serviceId 索引的 policy 快照，全局唯一实例，在 `AegisLoadBalancerAutoConfiguration` 注册 |
| `loadbalance/ConsistentHashReactiveLoadBalancer` | `ReactiveLoadBalancer<ServiceInstance>` 实现，按 serviceId 装配在 `AegisNamespaceLoadBalancerClientConfiguration` 中；内部持有 `RoundRobinLoadBalancer` 作为"policy 未配置"和"key 缺失降级"两种场景的统一委托对象；环用 Caffeine `Cache<String, ConsistentHashRing>`（`maximumSize(2)`）按实例集合+权重+虚拟节点数缓存，避免每次请求重建 |
| `loadbalance/HashKeyMissingLogger` | 按 serviceId 限流的 WARN 日志（默认 30s 窗口），key 缺失降级时提示配置可能有误 |

`AegisFilterOrder.LOAD_BALANCER = 10100` 仍然是预留但从未使用的顺序常量——一致性哈希是通过标准 `ReactiveLoadBalancer` 扩展点接入的（与命名空间感知 Supplier 同一挂载方式），不依赖这个 Filter 顺序常量，本次改动也没有采用它。
```

- [ ] **Task 9 Step 2: 更新 docs/rules/architecture-overview.md**

在 `docs/rules/architecture-overview.md` 的模块职责表格中，把：

```
| `gateway-loadbalancer` | 基于 Nacos + Spring Cloud LoadBalancer 的服务发现负载均衡 |
```

改为：

```
| `gateway-loadbalancer` | 基于 Nacos + Spring Cloud LoadBalancer 的服务发现负载均衡；支持按服务粒度可选启用一致性哈希策略（会话保持） |
```

- [ ] **Task 9 Step 3: Commit**

```bash
git add gateway-loadbalancer/CLAUDE.md docs/rules/architecture-overview.md
git commit -m "docs(loadbalancer): document consistent hash load balancing capability"
```

---

## 自动化任务完成后的整体验证

- [ ] Run: `./gradlew :gateway-loadbalancer:test`
      Expected: BUILD SUCCESSFUL，全部测试通过（Task 1~8 新增的全部测试类 + 原有测试类）
- [ ] Run: `./gradlew :gateway-server:bootJar`
      Expected: BUILD SUCCESSFUL（确认新增代码不破坏整体编译，`gateway-server` 依赖 `gateway-loadbalancer`）

---

## 端到端手动验证（可选，需本地 Nacos，不属于自动化测试范围）

以下步骤对应 `openspec/changes/add-consistent-hash-loadbalancer/tasks.md` 第 6 节，需要本地/测试环境的 Nacos 实例，人工执行、人工判断结果，不作为可自动勾选的 Task：

1. 启动本地 Nacos，注册一个测试服务（如 `demo-service`）的多个实例（建议至少 3 个，端口不同即可模拟多实例）。在 `aegis-governance.json` 中为该 serviceId 追加一条 `loadBalancePolicies` 记录（`keySource: HEADER`，`keyName` 任选，如 `X-Test-Key`），观察网关日志确认 governance 配置同步成功（无 `Failed to parse load balance policies` 错误日志）。
2. 用固定 `X-Test-Key` 值连续发送多次请求到经过该路由的网关地址，观察上游访问日志（或在测试服务里打印收到请求的实例标识），确认所有请求都落到同一个实例；换一个 `X-Test-Key` 值，确认（大概率）落到不同实例。
3. 发送不带 `X-Test-Key` 头的请求，确认：(a) 请求仍然正常被路由（降级到轮询，不报错）；(b) 网关日志在 30 秒窗口内只打印一条 `Consistent hash key missing, degraded to round-robin` WARN（短时间内连续发多个无 key 请求验证限流生效，而不是每个请求都打一条）。
4. 从 Nacos 下线其中一个实例（或调整其权重为 0），重新用步骤 2 中记录的多个 `X-Test-Key` 值发请求，确认：只有原本命中被下线实例的那些 key 对应的路由结果发生变化，其余 key 命中的实例不变。
5. 从 `aegis-governance.json` 移除该 serviceId 的 policy 条目（或整体不下发 `loadBalancePolicies`），确认下次 governance 同步后请求自动恢复为轮询分布（不需要重启网关或重发路由）。

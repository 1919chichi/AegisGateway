---
comet_change: add-consistent-hash-loadbalancer
role: technical-design
canonical_spec: openspec
archived-with: 2026-08-12-add-consistent-hash-loadbalancer
status: final
---

# 一致性哈希负载均衡设计文档

需求与验收场景的唯一事实源是 OpenSpec：`openspec/changes/add-consistent-hash-loadbalancer/proposal.md`、`design.md`（高层决策）、`specs/consistent-hash-loadbalancer/spec.md`（7 条 Requirement）。本文档只补充落到具体类结构、接口签名、数据结构、算法细节和测试策略层面的技术设计，不重复定义需求。

## 背景与约束

`gateway-loadbalancer` 目前只替换了 Spring Cloud LoadBalancer 的实例列表阶段（`ServiceInstanceListSupplier`，见 `NamespaceAwareNacosServiceInstanceListSupplier`），真正"从候选列表选一个实例"这一步完全交给 SCG 默认自动装配的 `RoundRobinLoadBalancer`。仓库中没有任何自定义 `ReactiveLoadBalancer`、哈希、环相关实现，这是一块全新能力。

关键现状约束（详见 open 阶段 `design.md` 的 Context）：
- Nacos SDK 是阻塞调用，所有 Nacos 交互跑在 `Schedulers.boundedElastic()` 上
- 实例列表是 pull-based，每次请求重新拉取，没有事件推送
- 失败一律 fail-open
- `AegisNamespaceLoadBalancerClientConfiguration` 已是按 serviceId 装配 Bean 的地方
- `gateway-ratelimit` 最近落地的 policy-based 分布式限流（commit `5e2f3cc`）建立了"governance policy + Nacos 热更新 + fail-open"的成熟范式，本设计直接复用其类结构风格（`RateLimitGovernanceConfig`/`RateLimitPolicy`/`RateLimitPolicyRepository`）

依赖排查结论（design 阶段确认）：`gateway-loadbalancer/build.gradle` 的编译 classpath 上**没有 Guava**；`netty-common`（含 `io.netty.util.internal.MurmurHash3`）作为 Nacos 客户端的传递依赖存在，但属于 `internal` 包，没有兼容性保证，不适合直接依赖。同时该模块已声明但从未使用 `caffeine` 依赖。这两点直接决定了下面 D9、D10 两个设计决策。

## 架构总览

```
                 ┌──────────────────────────────────────────┐
                 │  AegisNamespaceLoadBalancerClientConfiguration │
                 │  （每个 serviceId 一个子容器）                    │
                 └───────────────┬──────────────────────────┘
                                 │ @Bean
                                 ▼
                 ┌──────────────────────────────────┐
                 │  ConsistentHashReactiveLoadBalancer │◀── LoadBalancePolicyRepository
                 │  implements ReactiveLoadBalancer<ServiceInstance> │    .findByServiceId(serviceId)
                 └───────────────┬──────────────────┘
                                 │ choose(request)
                 policy==null 或 key 缺失 │           policy!=null 且 key 存在
                                 ▼                              ▼
                 ┌───────────────────────┐      ┌──────────────────────────────┐
                 │ 委托 RoundRobinLoadBalancer │      │ Caffeine Cache<String, ConsistentHashRing> │
                 │ （SCG 默认行为，逐字节保留） │      │ key = 排序实例串 + virtualNodesPerWeight     │
                 └───────────────────────┘      └───────────────┬──────────────┘
                                                                  │ 未命中 → loader 构建
                                                                  ▼
                                                  ┌───────────────────────────┐
                                                  │ ConsistentHashRing.build() │
                                                  │ MurmurHash3 + TreeMap 环   │
                                                  └───────────────────────────┘
```

`LoadBalancePolicyRepository` 独立监听 `aegis-governance.json` 的 `loadBalancePolicies` 节点（通过 `NacosConfigSyncService.registerGovernanceListener`），与 `ConsistentHashReactiveLoadBalancer` 解耦——后者每次请求只做一次 `AtomicReference` 读 + `Map.get`，配置热更新对请求路径而言是无感的。

## 组件设计

### 1. `hash.MurmurHash3`（手写最小实现）

**决策 D9**：手写 32-bit MurmurHash3 算法（`hash(byte[] data, int seed) -> int`），不引入 Guava，不依赖 `netty-common` 的 internal 包。理由见"背景与约束"。这是一个无状态的纯函数工具类，只在 `ConsistentHashRing` 构建虚拟节点位置时调用，不对外暴露为公共 API 契约（包内可见即可）。

### 2. `hash.ConsistentHashRing`

```java
public final class ConsistentHashRing {
    public static ConsistentHashRing build(List<ServiceInstance> instances, int virtualNodesPerWeight);
    public Optional<ServiceInstance> route(String key);
}
```

- 内部用 `TreeMap<Long, ServiceInstance>` 表示环；每个实例的虚拟节点数 = `virtualNodesPerWeight × 实例 weight`（weight 缺省按 1 处理，Nacos `ServiceInstance` 的 metadata/host 信息里可取到）
- 虚拟节点位置 = `MurmurHash3.hash((instanceId + "-" + i).getBytes(UTF_8), SEED)`，`instanceId` 用 `host:port` 拼接，保证同一物理实例在不同环重建间位置稳定（不依赖对象引用）
- `route(key)`：对 key 做同样的哈希，`TreeMap.ceilingEntry`，为空则 wrap-around 到 `firstEntry`；`instances` 为空列表时 `build()` 返回一个空环，`route()` 恒返回 `Optional.empty()`
- **环只从调用时传入的 `instances` 构建**，不做任何"过滤已下线实例"的运行时判断——这是 spec Requirement「候选实例不可用时的环上查找」的实现方式：被移除的实例根本不会出现在新环里，原本落在它虚拟节点区间的 key 在重建后自然计算到顺时针最近的、仍然存在的虚拟节点上。不需要额外的可用性检查分支

### 3. `hash.HashKeyExtractor`

```java
public interface HashKeyExtractor {
    Optional<String> extract(Request<?> request, LoadBalancePolicy policy);
}
```

- 提供一个统一实现（非策略模式的多个类），内部按 `policy.keySource()` 分支：`CLIENT_IP` 从 `RequestData` 的 remote address / `X-Forwarded-For` 取值；`HEADER` 从 `RequestData.getHeaders().getFirst(policy.keyName())` 取值
- 复用 `NamespaceAwareNacosServiceInstanceListSupplier.extractAttributes()` 同款的 `RequestDataContext` 解包方式，保持跨模块一致的请求上下文访问约定
- 取不到值（header 未携带、context 类型不符等）一律返回 `Optional.empty()`，不抛异常——调用方据此触发降级

### 4. `loadbalance.LoadBalancePolicy` / `LoadBalanceGovernanceConfig`

```java
public record LoadBalancePolicy(
        String serviceId,
        LoadBalanceStrategy strategy,          // 枚举：目前只有 CONSISTENT_HASH
        HashKeySource keySource,               // 枚举：CLIENT_IP / HEADER
        String keyName,                        // keySource=HEADER 时必填
        Integer virtualNodesPerWeight          // 可选，null 时用 DEFAULT_VIRTUAL_NODES_PER_WEIGHT=160
) {}

public record LoadBalanceGovernanceConfig(List<LoadBalancePolicy> loadBalancePolicies) {
    public LoadBalanceGovernanceConfig {
        loadBalancePolicies = loadBalancePolicies == null ? List.of() : List.copyOf(loadBalancePolicies);
    }
}
```

`aegis-governance.json` 片段：

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

**决策 D10**：`LoadBalancePolicy` 直接以 `serviceId` 为键，不像 `RateLimitPolicy` 那样通过路由 metadata 里的 `policyId` 间接引用——因为 Spring Cloud LoadBalancer 本身就是按 serviceId 一对一装配 `ReactiveLoadBalancer` Bean 的，没有"多个路由共享同一份负载均衡配置"的场景需要间接层。

### 5. `loadbalance.LoadBalancePolicyRepository`

结构完全镜像 `RateLimitPolicyRepository`：

```java
public class LoadBalancePolicyRepository {
    public LoadBalancePolicyRepository(NacosConfigSyncService syncService, ObjectMapper objectMapper) {
        syncService.registerGovernanceListener(this::onGovernanceUpdate);
    }
    public Optional<LoadBalancePolicy> findByServiceId(String serviceId);
    private void onGovernanceUpdate(String json) { /* 解析 + validate，失败保留旧快照 */ }
}
```

- 初始快照由 `registerGovernanceListener` 回放，**不在构造器里自行 get 初值**（与并发 Nacos 推送有覆盖竞态，这是 `RateLimitPolicyRepository` 类注释里已明确记录的教训）
- 内部 `AtomicReference<Map<String, LoadBalancePolicy>>`，key 为 `serviceId`
- 校验规则：`serviceId` 非空且批内不重复；`strategy`/`keySource` 非空（未知枚举值触发 Jackson 反序列化异常，走 catch-all fail-open）；`keySource==HEADER` 时 `keyName` 非空；`virtualNodesPerWeight` 若提供必须 > 0。任一校验失败 → 保留旧快照、记录 error 日志，不影响其他 serviceId 的现有配置

### 6. `loadbalance.ConsistentHashReactiveLoadBalancer`

> **修正（final-review C1）**：必须实现 `ReactorServiceInstanceLoadBalancer`（`org.springframework.cloud.loadbalancer.core`），
> 不能只实现上层的 `ReactiveLoadBalancer`。SCG 的 `ReactiveLoadBalancerClientFilter` 是按
> `clientFactory.getInstance(serviceId, ReactorServiceInstanceLoadBalancer.class)` 取 Bean 的；只实现上层接口会导致
> 这个查找永远匹配不到本类，SCG 默认的 `RoundRobinLoadBalancer` 会被静默保留使用，且没有任何报错。涉及框架扩展点时，
> 除了核实"要调的 API 存在"，还要核实"框架按什么类型来找这个 Bean"。

```java
public class ConsistentHashReactiveLoadBalancer implements ReactorServiceInstanceLoadBalancer {

    private final RoundRobinLoadBalancer delegate;
    private final ObjectProvider<ServiceInstanceListSupplier> supplierProvider;
    private final String serviceId;
    private final LoadBalancePolicyRepository policyRepository;
    private final HashKeyExtractor keyExtractor;
    private final HashKeyMissingLogger missingLogger;
    private final Cache<String, ConsistentHashRing> ringCache; // Caffeine, maximumSize(2)

    @Override
    public Mono<Response<ServiceInstance>> choose(Request request) {
        LoadBalancePolicy policy = policyRepository.findByServiceId(serviceId).orElse(null);
        if (policy == null) {
            return delegate.choose(request);
        }
        return supplierProvider.getIfAvailable().get(request).next().flatMap(instances -> {
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
}
```

关键点：
- **不再按 serviceId 条件装配两种 Bean**——`delegate` 是一个内部持有的 `RoundRobinLoadBalancer`（构造方式与 Spring Cloud LoadBalancer 自身创建默认实例完全一致：`new RoundRobinLoadBalancer(supplierProvider, serviceId)`）。"未配置 policy" 和 "key 缺失降级" 两条需求都通过委托同一个对象满足，不引入额外分支逻辑
- `buildCacheKey`：`排序后的 "host:port:weight" 拼接串 + "|vn=" + resolveVirtualNodes(policy)`。用**精确字符串**做 Caffeine 缓存 key，不做哈希摘要——不相等就是不相等，没有碰撞概率问题；`virtualNodesPerWeight` 参与 key 计算，保证该参数单独变化时也能触发重建（对应 spec 里新增的 Scenario）
- Caffeine `Cache<String, ConsistentHashRing>`，`maximumSize(2)`：只需要保留"当前"和"上一个"两份即可覆盖切换瞬间的并发读，`get(key, loader)` 语义天然是线程安全的"命中直接返回、未命中调用 loader 构建"，不需要手写 `AtomicReference` + 签名比较 + 原子替换的样板代码
- Nacos 实例查询本身已经在 `NamespaceAwareNacosServiceInstanceListSupplier` 内部跑在 `boundedElastic` 上，`choose()` 这一层不需要额外调度

### 7. `HashKeyMissingLogger`

```java
final class HashKeyMissingLogger {
    private final ConcurrentHashMap<String, AtomicLong> lastLoggedAt = new ConcurrentHashMap<>();
    private final long windowMillis; // 默认 30_000

    void warnIfDue(String serviceId, LoadBalancePolicy policy) {
        long now = System.currentTimeMillis();
        AtomicLong last = lastLoggedAt.computeIfAbsent(serviceId, k -> new AtomicLong());
        long prev = last.get();
        if (now - prev >= windowMillis && last.compareAndSet(prev, now)) {
            log.warn("Consistent hash key missing, degraded to round-robin. serviceId={}, keySource={}, keyName={}",
                    serviceId, policy.keySource(), policy.keyName());
        }
    }
}
```

`compareAndSet` 保证并发请求同时触发时只有一个线程真正打印这一条日志，其余线程直接跳过（不需要额外锁）。

### 8. Bean 装配（`AegisNamespaceLoadBalancerClientConfiguration`）

新增一个 `@Bean`，与现有 `aegisServiceInstanceListSupplier` 方法并列：

> **修正（final-review C1）**：返回类型和条件同样要用 `ReactorServiceInstanceLoadBalancer`，裸 `@ConditionalOnMissingBean`
> （隐式类型 = 方法返回类型）才能与 SCG 默认 Bean（隐式类型 `ReactorLoadBalancer`）互斥；本配置类先于 SCG 默认配置类注册
> （`NamedContextFactory#registerBeans` 的 "default." 前缀分支），本 Bean 先注册即先生效。

```java
@Bean
@ConditionalOnMissingBean
public ReactorServiceInstanceLoadBalancer aegisConsistentHashLoadBalancer(
        Environment environment,
        LoadBalancerClientFactory clientFactory,
        LoadBalancePolicyRepository policyRepository) {
    String serviceId = environment.getProperty(LoadBalancerClientFactory.PROPERTY_NAME);
    Assert.hasText(serviceId, "'serviceId' must not be empty");
    ObjectProvider<ServiceInstanceListSupplier> supplierProvider =
            clientFactory.getLazyProvider(serviceId, ServiceInstanceListSupplier.class);
    return new ConsistentHashReactiveLoadBalancer(supplierProvider, serviceId, policyRepository);
}
```

`@ConditionalOnMissingBean(ReactiveLoadBalancer.class)` 镜像现有 Supplier Bean 的写法，保证用户如果在自己的配置里提供了自定义 `ReactiveLoadBalancer`，本模块不会覆盖它。`LoadBalancePolicyRepository` 作为独立 Bean 注册在 `AegisLoadBalancerAutoConfiguration`（全局唯一实例，被所有 serviceId 的 LB 子容器共享读取）。

## 数据流：一次带 header 的请求

1. 客户端请求带 `X-User-Id: u10086`，路由到 `lb://order-service`
2. SCG 调用 `order-service` 对应的 `ConsistentHashReactiveLoadBalancer.choose(request)`
3. `policyRepository.findByServiceId("order-service")` 命中一条 `keySource=HEADER, keyName=X-User-Id` 的 policy
4. `supplierProvider.get(request)` 拿到当前候选实例列表（仍然是 `NamespaceAwareNacosServiceInstanceListSupplier` 产出的，命名空间/分组逻辑不变）
5. `keyExtractor.extract` 从 header 里取到 `"u10086"`
6. `buildCacheKey` 算出当前实例集合+虚拟节点数对应的缓存 key，Caffeine 命中（大概率，因为实例集合通常不会频繁变化）→ 直接拿到缓存的 `ConsistentHashRing`
7. `ring.route("u10086")` 返回目标实例，包成 `DefaultResponse` 返回给 SCG，后续走正常代理流程

## 错误处理与降级路径

| 场景 | 行为 |
|---|---|
| serviceId 未配置 policy | 委托 `RoundRobinLoadBalancer`，与本次改动前完全一致 |
| policy 存在但 key 提取为空 | 限流 WARN 日志 + 委托 `RoundRobinLoadBalancer` |
| governance 配置解析/校验失败 | `LoadBalancePolicyRepository` 保留旧快照，不影响其他 serviceId |
| 候选实例列表为空（Nacos 查询失败或确实无实例） | `ConsistentHashRing.build(List.of(), n)` 返回空环，`route()` 返回空，包成 `EmptyResponse`——与 SCG 默认 `RoundRobinLoadBalancer` 在无实例时的行为一致，不新增异常路径 |
| 候选实例被移除 | 下次请求触发环重建（cache key 变化），历史上落在该实例虚拟节点区间的 key 自动路由到顺时针下一个可用实例；此前已经建立的连接不受影响（不属于本设计范围） |

以上所有分支都不抛出异常中断请求，延续仓库 fail-open 惯例。

## 测试策略

- **`ConsistentHashRingTest`**：
  - 虚拟节点数按 `virtualNodesPerWeight × weight` 精确计算
  - 固定实例集合下，相同 key 多次 `route()` 结果一致
  - **确定性重映射断言**（对应 open 阶段 Open Question 的落地方案）：构造 5 个实例、10000 个随机 key，记录初始映射；移除 1 个实例后重建环，重新记录映射；断言「初始映射目标仍在新环里的 key，其映射结果 100% 不变」，且「映射发生变化的 key」全部原来命中被移除的那个实例；同时统计变化比例应落在 `1/5` 量级的合理区间（如 15%~30%），作为交叉印证而非唯一判定依据
  - 空实例列表：`route()` 返回 `Optional.empty()`
- **`LoadBalancePolicyRepositoryTest`**：镜像 `RateLimitPolicyRepositoryTest` 风格——正常解析、serviceId 重复报错并保留旧快照、`HEADER` 缺 `keyName` 报错、`virtualNodesPerWeight<=0` 报错
- **`ConsistentHashReactiveLoadBalancerTest`**：
  - 未配置 policy → 行为等同 `RoundRobinLoadBalancer`（可通过多次调用观察轮询顺序，或直接验证委托对象被调用）
  - 配置 policy 且 key 存在 → 相同 key 稳定路由到同一实例
  - key 缺失 → 降级轮询，且不抛异常
  - 限流日志：短时间内多次 key 缺失只触发一次 `warnIfDue` 的实际打印（可通过注入固定 `windowMillis` 和可控时钟测试，或直接断言 `AtomicLong` 内部状态）
  - 移除实例后，仅原本命中该实例的 key 改变路由结果
- 全部为纯单元测试，不需要真实 Nacos，`ServiceInstance`、`Request<RequestDataContext>` 均用测试替身构造，延续 `NamespaceAwareNacosServiceInstanceListSupplierTest` 的既有测试风格

## 迁移与回滚

与 open 阶段 `design.md` 的 Migration Plan 一致：纯新增能力，`aegis-governance.json` 新增字段非破坏性；上线先在单个非核心 serviceId 验证，观察降级日志确认符合预期后再扩大范围；回滚只需从 `loadBalancePolicies` 移除对应条目，下次 governance 同步后自动恢复默认轮询，无需重启或重发路由。

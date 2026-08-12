---
change: add-consistent-hash-loadbalancer
verify_mode: full
verified_at: 2026-08-12
---

# 验证报告：一致性哈希负载均衡（add-consistent-hash-loadbalancer）

## Summary

| Dimension    | Status |
|--------------|--------|
| Completeness | 28/28 tasks.md 任务完成（含 final review 修复轮次追加的检查项）；7/7 delta spec requirement 有对应实现 |
| Correctness  | 7/7 requirement 场景有实现和测试证据；1 处 spec 文字与实现不一致已在本次验证中修正 |
| Coherence    | 1 处 design.md 决策（D4）与实际实现机制不一致，已在本次验证中以 Implementation Divergence 说明记录；其余决策（D1-D3, D5-D8）与实现一致 |

代码范围：`bdb48a6..4a0419b`，22 commits，25 个文件变更（+1820/-9 行），全部在 `gateway-loadbalancer` 模块 + 两处文档。

## Completeness

**Task Completion**：`openspec/changes/add-consistent-hash-loadbalancer/tasks.md` 全部可勾选项已勾选（1.3 因设计决策变更标注为 superseded 并从 checkbox 列表移出，非遗漏；第 6 节手动端到端验证按 plan 文档定义为"非自动化范围"，第 7 节为 final review 发现、用户确认接受的后续跟进项，均已从阻塞性 checkbox 转为非阻塞说明性条目，理由见 verify 阶段与用户的沟通记录）。

**Spec Coverage**（7 个 requirement 逐一核实）：

| Requirement | 实现证据 |
|---|---|
| 按服务粒度的策略开关 | `LoadBalancePolicyRepository`（`gateway-loadbalancer/.../loadbalance/LoadBalancePolicyRepository.java`）+ `AegisNamespaceLoadBalancerClientConfiguration.aegisConsistentHashLoadBalancer` 按 serviceId 装配 |
| 基于可配置 key 的一致性哈希路由 | `HashKeyExtractor`/`DefaultHashKeyExtractor` + `ConsistentHashRing.route()` |
| 虚拟节点与权重分配 | `ConsistentHashRing.build()` + `resolveWeight()` |
| 哈希环缓存与按需重建 | `ConsistentHashReactiveLoadBalancer` 内的 Caffeine `Cache<String, ConsistentHashRing>`（机制与 design.md D4 描述不同，见下方 Coherence） |
| Key 缺失时的降级与限流日志 | `ConsistentHashReactiveLoadBalancer.chooseInternal` + `HashKeyMissingLogger` |
| 候选实例不可用时的环上查找 | `ConsistentHashRing.route()` 的 `ceilingEntry`/`firstEntry` wrap-around |
| 与重试机制的独立性 | 设计层面天然满足——`ConsistentHashReactiveLoadBalancer` 不读取/不依赖任何 SCG 重试相关状态，代码中未出现相关引用 |

全部 7 个 requirement 均有对应实现，无缺失。

## Correctness

**Requirement Implementation Mapping**：与上表一致，均已在 9 个任务的单任务审查 + 全分支最终审查（含一次 Critical 修复轮次）中逐一核实过实现与需求的对应关系，本次未发现新的映射缺口。

**Scenario Coverage**：

- 7 个 requirement 下的全部场景均有对应实现路径。
- **WARNING（已记录为 deferred，tasks.md 7.5）**："实例集合未变化时复用缓存环"和"Policy 参数变化触发重建"两条场景的实现逻辑正确（Caffeine cache key 含 `|vn=` 后缀，天然满足），但缺少**独立**测试直接锁定这两条场景本身（现有测试验证的是路由结果稳定性，逻辑上蕴含但不直接证明"复用/重建"这个中间行为）。不阻塞本次验证通过，建议后续任务补上。
- **已修正**：`strategy: consistent-hash`（kebab-case）与实现的 `CONSISTENT_HASH` 不一致，已在本次验证中修正 spec.md 文字（`specs/consistent-hash-loadbalancer/spec.md:11`）。

## Coherence

**Design Adherence**：

- D1（`ReactiveLoadBalancer` Bean 挂载点）、D2（哈希环+虚拟节点算法）、D3（可插拔 HashKeyExtractor + 限流日志）、D5（governance policy 载体）、D6（权重支持）、D7（环上顺时针查找）、D8（与 RETRY 独立）均与实现一致。
- **注意**：D1 提到的接口类型是 `ReactiveLoadBalancer<ServiceInstance>`，proposal.md 也是这个措辞。全分支最终评审发现这个类型本身是个 Critical bug（SCG 实际按更具体的 `ReactorServiceInstanceLoadBalancer` 子接口解析 Bean，只实现上层接口会导致特性静默不生效），已在 fix commit `4a0419b` 修正为 `implements ReactorServiceInstanceLoadBalancer`，并补了一条走真实 `LoadBalancerClientFactory.getInstance()` 解析路径的集成测试防止回归。design.md/proposal.md 未同步更新这个类型细节——不算功能性偏离（两者描述的挂载点和职责边界没变，只是具体接口类型这个实现细节比 open 阶段设想的更具体），本次验证不要求为此单独更新 design.md（技术设计文档 `docs/superpowers/specs/2026-08-12-consistent-hash-loadbalancer-design.md` 已经是正确的，OpenSpec 归档时以技术设计文档和实际代码为准）。
- **已处理**：D4（环生命周期管理机制：签名比较 + 手写原子替换）与实际实现（Caffeine `Cache<String, ConsistentHashRing>`）不一致。用户已确认选择"在 design.md 追加 Implementation Divergence 说明"，已完成（`design.md` "Implementation Divergence" 一节，记录了变更原因、行为等价性论证、已知局限）。

**Code Pattern Consistency**：全部新代码严格镜像 `gateway-ratelimit` 的 policy-based 限流实现风格（governance policy record + repository + Nacos 热更新 + fail-open），Javadoc 规范符合项目 `docs/rules/comment-conventions.md`。未发现值得指出的模式偏离。

## Issues

### CRITICAL（必须修复才能归档）
无。（原本发现的 1 个 Critical——C1 接口类型错误导致特性生产环境 no-op——已在 build 阶段的 final review 修复轮次中修复并通过 scoped re-review，证据见 `.superpowers/sdd/2026-08-12-consistent-hash-loadbalancer/final-review.md` 与 `final-fix-rereview.md`。）

### WARNING（建议修复，不阻塞本次归档）
1. **`ConsistentHashRingTest`/`ConsistentHashReactiveLoadBalancerTest` 缺少"缓存复用"与"vn 变化触发重建"的独立测试** —— 已记录为 `tasks.md` 7.5，deferred。
2. **`virtualNodesPerWeight` 无上界校验，极端配置可能导致内存/CPU 问题** —— 已记录为 `tasks.md` 7.1，deferred（final review I2）。
3. **Caffeine 环缓存 `maximumSize(2)` 在 gateway-gray 多 namespace 场景下可能抖动，缓存 key 未含 namespace/group** —— 已记录为 `tasks.md` 7.2，deferred（final review I5）。

### SUGGESTION（可选）
1. `LoadBalanceStrategy` 未来新增枚举值时 `choose()` 未显式校验 strategy —— 已记录为 `tasks.md` 7.3。

## Final Assessment

无 CRITICAL 问题。3 个 WARNING 均已记录为后续跟进任务（`tasks.md` 第 7 节），用户已确认接受、不阻塞本次归档。Coherence 检查发现的 1 处 design.md 与实现的分歧已通过 Implementation Divergence 说明处理；1 处 spec.md 文字错误已直接修正。**可以进入分支收尾流程。**

# gateway-ratelimit

基于 Redisson（Redis）的分布式限流模块。作为 autoconfigure 库被 `gateway-server` 组装，向网关注入一个 `RateLimitFilter`，按路由绑定的限流策略对请求扣令牌。

> 全局架构、构建命令、注释规范见根 `CLAUDE.md` 与 `docs/rules/`，本文件只描述本模块特有内容。

## 核心设计约束

- **网关启动不依赖 Redis**：刻意使用 `org.redisson:redisson` 核心库而非 `redisson-spring-boot-starter`（starter 会在启动时立即建连，Redis 不可用即拖垮网关）。Redis 客户端由 `RedissonClientManager` 按治理配置惰性创建。
- **配置了限流才用 Redis**：没有任何限流策略时，即便配了 `rateLimitRedis` 也不保持连接。
- **fail-open**：路由未绑定策略、策略不存在、Redis 未配置/不可用、扣令牌出错——一律放行。限流是保护手段，不能成为新的单点故障。

## 关键类与职责

| 类 | 职责 |
|---|---|
| `config/RateLimitAutoConfiguration` | 自动配置入口。`@ConditionalOnBean(NacosConfigSyncService.class)`、`@AutoConfiguration(after = AegisCoreAutoConfiguration.class)`。刻意不以 Redisson bean 为条件。注册下列 4 个 bean（均 `@ConditionalOnMissingBean`）。|
| `filter/RateLimitFilter` | `GlobalFilter`，顺序 `AegisFilterOrder.RATE_LIMIT`。从路由 metadata 取 `rateLimit.policyId`，逐条扣令牌，任一拒绝写 429。|
| `repository/RateLimitPolicyRepository` | 监听 Nacos governance，维护策略内存快照（`AtomicReference<Map>`），并驱动 `RedissonClientManager` 生命周期。|
| `core/RateLimitKeyResolver` | 计算请求命中的规则并生成 Redis key。|
| `core/RedissonClientManager` | 惰性管理 Redisson 客户端生命周期（`AutoCloseable`，`destroyMethod = "close"`）。|
| `core/ReactiveRateLimiterGateway` / `RedissonReactiveRateLimiterGateway` | 限流后端抽象 + Lua 令牌桶实现。|
| `core/MatchedRateLimitRule` | record：一条命中规则 + 已解析的 key。|
| `model/*` | 策略/规则/类型/Redis 配置/治理配置模型（均为 record）。|

## 模型与限流维度

治理配置 `aegis-governance.json` 中本模块关心两个节点（`RateLimitGovernanceConfig`）：

- `rateLimitPolicies`：`RateLimitPolicy(id, rules)` 列表。路由通过 metadata `rateLimit.policyId` 绑定到某个策略组，规则细节全在组内，路由配置不随限流细节变化。`rules` 保持配置顺序（同类型内的执行顺序依赖它）。
- `rateLimitRedis`：`RateLimitRedisConfig(address, password, database)`。`address` 必须以 `redis://` 或 `rediss://` 开头；`database` 缺省归一化为 0。record 的 `equals` 即为「变更指纹」，供 `RedissonClientManager` 判断是否需重建客户端。`null` 表示未配置 → 所有规则 fail-open。

`RateLimitRule(id, type, capacity, refillRate, pathPattern, identityHeader)`：

- `type` 为 `RateLimitType` 枚举，三个维度各自持有对应的 429 业务错误码（`AegisErrorCode.RATE_LIMIT_{SERVICE,PATH,USER}`），新增维度时编译器强制给出错误码。
- `capacity` = 突发量（桶容量），`refillRate` = 每秒补充令牌数（近似稳定 QPS）。两者随 Lua 脚本每次下发，**配置热更新后新请求立即生效**，无需同步 Redis 侧状态。
- `pathPattern`：PATH 规则的 Spring `PathPattern`，匹配 StripPrefix 前的原始路径。
- `identityHeader`：USER 规则取标识的请求头，缺省 `X-User-Id`。

校验（`RateLimitPolicyRepository.validate`）：策略/规则 id 非空且组内去重，`type` 非空，`capacity`/`refillRate` 必须 > 0，PATH 规则必须有合法 `pathPattern`，`rateLimitRedis` 可缺失但写了就必须地址合法。任一校验失败 → 保留旧快照、不触碰客户端状态，坏配置不会打掉正在生效的限流。

## Filter 执行流程

1. 取路由 metadata `rateLimit.policyId`，无则放行。
2. `repository.findById(policyId)`，策略不存在则 warn 放行。
3. `RateLimitKeyResolver.resolve()` 计算命中规则，命中顺序固定 **USER → PATH → SERVICE**（同类型内按配置顺序）。多桶 AND 语义：更细粒度、更易拒绝的规则先扣，多桶无回滚时浪费的令牌最少。
4. 逐条 `limiterGateway.tryAcquire(key, rule)`：
   - `true` → 扣下一条；
   - `false` → 直接写 429（用 `ApiErrorResponseWriter` + 该维度错误码，**不抛异常**，因为 `GlobalExceptionHandler` 无法区分 429 的限流维度）；
   - 空 Mono / error → fail-open 放行。

## key 解析（RateLimitKeyResolver）

- 前缀 `aegis:ratelimit:<policyId>:<ruleId>`。
- PATH 桶由 ruleId 唯一确定，pathPattern 不进 key；SERVICE 追加 `serviceId`（`lb://` 路由取 host，否则取 routeId）；USER 追加 sanitize + 截断（最长 64）后的 identity，缺省 `anonymous`。
- 安全：`{` `}` `:` 统一替换为 `_`（`{}` 在 Redis Cluster 是 hash tag，`:` 破坏 key 分段）；USER identity 来自外部 header，截断防巨型 key。
- 编译后的 `PathPattern` 走 `ConcurrentHashMap` 缓存，模式只来自已校验配置，集合天然有界，不能在每请求热路径重复编译。

## 惰性 Redisson 客户端（RedissonClientManager）

- 客户端只在「存在限流策略且配了 `rateLimitRedis`」时创建；策略清空或配置移除时关闭。
- Redis 配置热更新：Redisson 不支持改活客户端地址，故「建新客户端 → 原子替换 → 关旧客户端」，旧客户端上未完成请求自然结束。
- 线程模型：`apply()` 仅由 Nacos governance 单线程虚拟线程 Executor 调用（可阻塞建连）；`reconcile()` 用 `synchronized` 串行化；请求路径只 volatile 读 `current()`，永不阻塞事件循环。
- 自愈：创建失败不抛异常，保留现状、期间 fail-open；通过下次治理推送，或请求路径上带冷却（默认 10s，`retryLater()` 在 `boundedElastic` 调度）的重建恢复。

## 令牌桶实现（RedissonReactiveRateLimiterGateway）

- 不用 Redisson `RRateLimiter`（桶容量恒等于 rate，无法表达「突发 capacity + 稳定 refillRate」）。
- 自带 Lua 令牌桶脚本（`RScript.Mode.READ_WRITE`）：时间取 **Redis 服务端时钟**（多网关实例无时钟偏差），每次执行 capacity/refillRate 随参数下发（热更新天然生效），每次重设 TTL（`max(桶补满周期, 60s)`），冷 key 自动过期防止 USER 维度 key 无限累积。
- 客户端缺失时返回空 Mono（顺带触发 `retryLater()`），调用方 fail-open。

## 与 gateway-core 的关系

依赖 `gateway-core`，复用：`NacosConfigSyncService.registerGovernanceListener()`（初始快照由它回放，**不要在构造器自行 get 初值**，否则与并发 Nacos 推送有覆盖竞态）、`AegisFilterOrder.RATE_LIMIT`、`AegisErrorCode`、`ApiErrorResponseWriter`、`AegisCoreAutoConfiguration`。

## 测试

```bash
# 全部
./gradlew :gateway-ratelimit:test
# 单类
./gradlew :gateway-ratelimit:test --tests "io.aegis.gateway.ratelimit.filter.RateLimitFilterTest"
```

测试类：`RateLimitAutoConfigurationTest`、`RateLimitFilterTest`、`RateLimitPolicyRepositoryTest`、`RateLimitKeyResolverTest`、`RedissonClientManagerTest`、`RedissonReactiveRateLimiterGatewayTest`。`RedissonClientManager` 提供包级测试构造器，可注入假客户端工厂与缩短冷却时间，避免真实建连与真实等待。

> 设计文档：`docs/superpowers/specs/2026-06-11-redisson-rate-limit-policy-design.md`、`docs/superpowers/plans/2026-06-11-redisson-rate-limit-policy.md`。

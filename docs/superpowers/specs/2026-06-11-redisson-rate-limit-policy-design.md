# Redisson 限流策略设计文档

## 目标

在 `gateway-ratelimit` 模块中实现基于 Redisson 的分布式限流。路由只绑定限流策略，不感知具体限流规则；具体的服务总量、URL、用户等限流项统一放在 `aegis-governance.json` 中维护，并支持 Nacos 热更新。

## 背景与约束

- `gateway-server` 已依赖 `gateway-ratelimit`，但该模块目前只有 `build.gradle`，没有源码实现。
- `gateway-core` 已预留 `AegisFilterOrder.RATE_LIMIT = -100`，语义是在认证之后、灰度路由之前执行。
- `gateway-core` 已定义 429 业务错误码：`RATE_LIMIT_PATH`、`RATE_LIMIT_SERVICE`、`RATE_LIMIT_USER`。
- Nacos 是配置唯一来源；治理配置通过 `aegis-governance.json` 分发，各模块自行解析自己的配置段。
- Redisson 用于执行 Redis Lua 令牌桶，多个网关实例共享 Redis 中的限流状态。
- **网关启动不依赖 Redis**：使用 Redisson 核心库而非 `redisson-spring-boot-starter`（starter 会在启动时立即建连，Redis 不可用导致整个网关起不来）。Redis 客户端由 `RedissonClientManager` 按治理配置惰性创建——没有限流策略时完全不使用 Redis。
- Redis 连接配置也放在 `aegis-governance.json`（`rateLimitRedis` 节点）而非 application.yml，与"Nacos 唯一来源"一致，并支持热更新。

## 配置模型

### 路由配置

路由只绑定一个限流策略组，不关心策略组内有哪些具体规则。

```json
{
  "routes": [
    {
      "id": "user-service-route",
      "uri": "lb://user-service",
      "predicates": ["Path=/api/users/**"],
      "filters": ["StripPrefix=1"],
      "order": 0,
      "metadata": {
        "rateLimit": {
          "policyId": "user-service-policy"
        }
      }
    }
  ]
}
```

- `metadata.rateLimit.policyId`：引用 `aegis-governance.json` 中的限流策略组。
- 路由不直接绑定某一条具体限流规则，避免路由配置随限流细节频繁变化。

### 治理配置

限流详细规则放在 `aegis-governance.json` 的 `rateLimitPolicies` 节点。

```json
{
  "rateLimitRedis": {
    "address": "redis://127.0.0.1:6379",
    "password": null,
    "database": 0
  },
  "rateLimitPolicies": [
    {
      "id": "user-service-policy",
      "rules": [
        {
          "id": "user-service-total",
          "type": "SERVICE",
          "capacity": 1000,
          "refillRate": 500
        },
        {
          "id": "user-login-path",
          "type": "PATH",
          "pathPattern": "/api/users/login",
          "capacity": 50,
          "refillRate": 10
        },
        {
          "id": "user-api-per-user",
          "type": "USER",
          "capacity": 60,
          "refillRate": 10,
          "identityHeader": "X-User-Id"
        }
      ]
    }
  ]
}
```

字段说明：

| 字段 | 说明 |
|---|---|
| `id` | 策略组或规则 ID，在同一层级内唯一 |
| `rules` | 一个策略组下的限流规则列表 |
| `type` | 限流维度，第一版支持 `SERVICE`、`PATH`、`USER` |
| `capacity` | 令牌桶容量，允许的短时突发请求量 |
| `refillRate` | 每秒补充令牌数，近似等于稳定 QPS |
| `pathPattern` | `PATH` 规则使用的请求路径匹配模式（Spring `PathPattern` 语法），不包含 query string |
| `identityHeader` | `USER` 规则的用户标识请求头，默认 `X-User-Id` |
| `rateLimitRedis.address` | Redis 地址，必须为 `redis://` 或 `rediss://` 前缀；缺失时所有限流规则 fail-open |
| `rateLimitRedis.password` | Redis 密码，null/空白表示无密码 |
| `rateLimitRedis.database` | Redis 逻辑库编号，默认 0 |

## 规则语义

### SERVICE

`SERVICE` 规则限制当前路由对应下游服务的总请求量。

- 适用范围：绑定该策略组的路由。
- key 维度：`policyId + ruleId + serviceId`。
- `serviceId` 优先从 `lb://service-name` 中提取；无法提取时退化为 routeId。

### PATH

`PATH` 规则限制某个请求 URL 模式。

- 只有当前请求 path 命中 `pathPattern` 时才生效。
- 匹配语义使用 Spring `PathPattern`（与 SCG 路由谓词一致），支持 `/api/orders/{id}`、`/api/orders/**` 等模式。
- 匹配对象是网关收到的**原始请求 path**：`RATE_LIMIT = -100` 先于 `StripPrefix` 等路由 filter 执行，此时 path 尚未被改写。
- query string 不参与匹配，也不进入 Redis key。
- key 维度：`policyId + ruleId`。`ruleId` 在策略组内已唯一确定一条 PATH 规则，`pathPattern` 不进入 key——既冗余，又会引入 `{`、`}`、`:` 等破坏 Redis key 结构的字符（见下文 key 安全约束）。
- 不建议为完整动态 URL 配独立规则，例如 `/api/orders/123` 不应产生一个独立桶；应通过 `/api/orders/{id}` 或 `/api/orders/*` 这类模式归一。
- 同一请求命中多条 PATH 规则时，所有命中规则都参与 AND 判断。

### USER

`USER` 规则限制单个用户的请求频率。

- 用户标识从 `identityHeader` 指定的请求头读取，默认 `X-User-Id`。
- 请求头缺失时使用 `anonymous`，表示所有未识别用户共享一个匿名桶。
- key 维度：`policyId + ruleId + identity`。
- **信任边界**：认证 filter（`AUTH = -200`）在限流之前执行，`X-User-Id` 必须由认证 filter 从 JWT 解析后注入并覆盖客户端传入值。否则客户端可伪造该 header 绕开自己的用户桶，并刷出无限多的 Redis key。
- identity 进入 key 前必须 sanitize 并限制长度（见下文 key 安全约束）。

## 执行流程

```text
请求进入网关
  -> SCG 匹配 Route
  -> RateLimitFilter 读取 Route metadata.rateLimit.policyId
  -> 从内存快照中找到对应 RateLimitPolicy
  -> 计算本次请求命中的 rules
  -> 按 USER -> PATH -> SERVICE 顺序逐个获取令牌（同类型内按配置顺序）
  -> 所有命中规则均获取成功：继续转发
  -> 任一命中规则获取失败：返回 HTTP 429 + ApiResponse
```

规则命中逻辑：

- `SERVICE`：总是命中。
- `PATH`：请求 path 匹配 `pathPattern` 时命中。
- `USER`：总是命中，但 key 会按用户标识区分。

放行逻辑使用 AND 语义：

```text
所有命中的限流规则都通过 => 请求放行
任意一条命中的限流规则失败 => 返回 429
```

这个语义可以同时保护不同层面的风险：

- 服务总量限流防止下游服务整体被打爆。
- URL 限流防止登录、短信、导出等高风险接口被单独打爆。
- 用户限流防止单个用户或 token 高频刷接口。

### 故障降级语义（fail-open）

限流是保护手段，不应成为新的单点故障。以下情况一律放行：

- 路由未配置 `metadata.rateLimit.policyId`：直接放行。
- `policyId` 在快照中不存在：放行并记录 warn 日志。
- 配置了限流策略但未配置 `rateLimitRedis`：放行，配置更新时记录一次 warn 日志。
- Redis 客户端建连失败（如 Redis 宕机时推送了配置）：放行并记录 error 日志，
  通过下一次配置推送或请求路径上带 10s 冷却的异步重试自愈。
- Redis 不可用或获取令牌异常/超时：放行并记录 error 日志。

启动期同理：网关启动完全不依赖 Redis——没有限流策略时根本不创建 Redis 客户端，
有策略但 Redis 不可用时网关照常启动，限流在 Redis 恢复后自动生效。

## Redisson 令牌桶设计

每条命中的规则对应一个 Redis 令牌桶 key。第一版不使用 Redisson `RRateLimiter`，因为 Redisson 4.4.0 的 `RRateLimiter` 只支持 `rate + interval`，内部桶容量也等于 `rate`，无法表达 `capacity=1000, refillRate=500` 这种“突发 1000、稳定 500/s”的配置语义。

**必须使用响应式 API**：统一通过 `RedissonReactiveClient.getScript()` 获取 `RScriptReactive` 并执行 Lua token bucket，保证 filter 全链路非阻塞。禁止在 `GlobalFilter` 的事件循环线程上调用阻塞 Redisson API。

Redis key 格式：

```text
aegis:ratelimit:{policyId}:{ruleId}:{dimensionValue}
```

`dimensionValue` 按规则类型取值：

| 类型 | dimensionValue |
|---|---|
| `SERVICE` | serviceId |
| `PATH` | 无（省略该段，桶由 ruleId 唯一确定） |
| `USER` | 用户标识 |

示例：

```text
aegis:ratelimit:user-service-policy:user-service-total:user-service
aegis:ratelimit:user-service-policy:user-login-path
aegis:ratelimit:user-service-policy:user-api-per-user:10001
```

key 安全约束：

- `dimensionValue` 来自配置或请求头，写入 key 前必须 sanitize：`{`、`}` 在 Redis Cluster 下会被解释为 hash tag、`:` 会破坏 key 分段，统一替换为 `_`。
- `USER` 维度的 identity 来自外部输入，需额外限制长度（超过 64 字符截断），防止恶意超长 header 生成巨型 key。

令牌桶参数：

- `capacity` 映射为桶容量，即最多允许累积的 token 数。
- `refillRate` 映射为每秒补充令牌数。
- 每个请求默认消耗 1 个令牌。

### Redis 客户端生命周期（RedissonClientManager）

客户端按"配置驱动、惰性创建"管理，这是"配置了限流才使用 Redis"约束的落点：

- 仅当 **存在限流策略且配置了 `rateLimitRedis`** 时创建客户端；策略清空或配置移除时关闭客户端。
- Redisson 不支持在存活客户端上修改服务端地址，**Redis 配置热更新 = 创建新客户端 → 原子替换 → 关闭旧客户端**；`RateLimitRedisConfig` 的 record equals 即变更指纹。新客户端建连失败时保留旧客户端继续服务。
- 建连是阻塞操作，只发生在 Nacos governance 虚拟线程或 boundedElastic 上，绝不在事件循环线程上。请求路径只做 volatile 读。
- 客户端缺失时 limiter gateway 返回空 Mono（"无决策"），filter 按 fail-open 放行。

### 配置热更新

filter 重新解析 `rateLimitPolicies` 并原子替换内存快照。Lua 每次请求都会读取当前规则的 `capacity` 和 `refillRate` 参数，因此不需要维护本地配置指纹，也不需要调用 Redisson `trySetRate` / `setRate` 覆盖 Redis 参数。

初始快照由 `NacosConfigSyncService` 在监听器注册时于串行 Executor 上回放，模块不得在注册后自行 get 初始值——那种"注册后再读取"的模式与并发到达的 Nacos 推送存在新值被旧快照覆盖的竞态。

### key 生命周期

令牌桶 key 会在 Lua 中显式设置过期时间。`USER` 维度每个用户一个 key，长期运行会累积大量冷用户 key；TTL 建议取 `max(capacity / refillRate, 60s)` 量级——足够覆盖桶的完整补充周期，又能让冷用户的 key 自动清理。

## 多规则扣令牌边界

第一版使用多个 Redis token bucket key 组合实现多维度限流。需要明确一个边界：多个 Redis key 之间没有天然的跨 key 原子扣减语义。

因此第一版采用保守策略：

- 按命中的规则逐个执行 Lua token bucket 扣减。
- 任意规则失败立即返回 429。
- 已经成功扣减的前置规则不回滚。

这会带来一个可接受但需要记录的现象：如果前面的规则扣令牌成功，后面的规则失败，本次请求会被拒绝，但前面规则的令牌已经消耗。它的结果是限流略微更保守，不会导致超放。

为降低这种影响，规则执行顺序按更容易拒绝、更细粒度的规则优先：

```text
USER -> PATH -> SERVICE
```

如果后续需要严格的多桶原子扣减，需要把同一次请求命中的所有桶合并到一段 Redis Lua 脚本里判断并扣减。

## 429 响应

限流失败时返回：

- HTTP status：`429 Too Many Requests`
- Content-Type：`application/json`
- Body：统一 `ApiResponse`

错误码按失败规则类型选择：

| 失败规则类型 | 错误码 |
|---|---|
| `PATH` | `RATE_LIMIT_PATH` / `42901` |
| `SERVICE` | `RATE_LIMIT_SERVICE` / `42902` |
| `USER` | `RATE_LIMIT_USER` / `42903` |

如果多个规则都可能失败，以第一个实际失败的规则为准。

**实现约束**：`GlobalExceptionHandler` 会把所有 429 的 `ResponseStatusException` 统一映射为 `RATE_LIMIT_PATH`，无法区分规则类型。因此 `RateLimitFilter` 必须自行写出 429 响应，**不得通过抛异常走全局异常链**，否则 SERVICE/USER 失败也会变成 42901。

响应写出统一走 gateway-core 的 `ApiErrorResponseWriter`（`GlobalExceptionHandler` 也使用它），错误响应结构只在一处维护。维度 → 错误码的映射由 `RateLimitType` 枚举自身持有，新增维度时编译器强制给出错误码。

## 模块设计

`gateway-ratelimit` 新增组件：

| 组件 | 职责 |
|---|---|
| `RateLimitAutoConfiguration` | 自动配置入口，注册 `RateLimitFilter` |
| `RateLimitFilter` | SCG `GlobalFilter`，执行限流判断和 429 响应 |
| `RateLimitPolicy` | 策略组模型，包含 `id` 和 `rules` |
| `RateLimitRule` | 单条规则模型 |
| `RateLimitType` | `SERVICE`、`PATH`、`USER` 枚举 |
| `RateLimitPolicyRepository` | 监听 Nacos governance 配置，维护策略快照并驱动客户端生命周期 |
| `RateLimitKeyResolver` | 根据请求、路由和规则生成 Redis key（PathPattern 编译结果缓存于此） |
| `ReactiveRateLimiterGateway` | Redis 访问抽象；空 Mono = 无决策（调用方 fail-open） |
| `RedissonReactiveRateLimiterGateway` | 使用 `RScriptReactive` 执行 Lua token bucket |
| `RedissonClientManager` | 按治理配置惰性创建/替换/关闭 Redisson 客户端 |
| `RateLimitRedisConfig` | `rateLimitRedis` 节点模型，equals 即客户端重建指纹 |

gateway-core 同步新增/调整：

- `ApiErrorResponseWriter`：统一的错误响应写出工具，`GlobalExceptionHandler` 与 `RateLimitFilter` 共用。
- `NacosConfigSyncService`：注册监听器时在串行 Executor 上回放当前快照，消除"注册后读取初始值"的竞态模式。

`RateLimitFilter` 只依赖内存策略快照和 Redisson，不直接访问 Nacos。

`RateLimitPolicyRepository` 解析失败（JSON 非法）或包含非法规则（`capacity`/`refillRate` 非正数、未知 `type`、同层级重复 `id`）时，保留旧快照并记录 error 日志，不让坏配置打掉现有限流。

## 测试计划

单元测试覆盖：

- 能从 `aegis-governance.json` 解析多个 `rateLimitPolicies`。
- 路由未配置 `metadata.rateLimit.policyId` 时直接放行。
- `policyId` 不存在时直接放行并记录 warn 日志。
- `SERVICE` 规则对整条路由生效。
- `PATH` 规则只在 path 命中 `pathPattern` 时生效。
- `USER` 规则按请求头生成用户维度 key，请求头缺失时使用 `anonymous`。
- 多条规则同时命中时，全部通过才放行。
- 任一规则失败时返回 429，并使用对应业务错误码。
- 规则执行顺序为 USER -> PATH -> SERVICE，同类型内按配置顺序。
- 限流失败时直接写出响应而非抛异常，SERVICE/USER 失败返回各自的错误码而非 42901。
- Redis 不可用或获取令牌异常时请求放行（fail-open）并记录 error 日志。
- USER identity 含 `{`、`}`、`:` 等特殊字符时被 sanitize，超长时被截断。
- filter 顺序等于 `AegisFilterOrder.RATE_LIMIT`。
- governance 配置热更新后，新请求使用新策略快照。
- 热更新修改 `capacity`/`refillRate` 后，新请求使用最新参数执行 Lua token bucket。
- governance 配置 JSON 非法或包含非法规则（含非法 Redis 地址）时保留旧快照并记录 error 日志。
- 无限流策略时不创建 Redis 客户端；策略与 Redis 配置齐备时才创建。
- Redis 配置变更时创建新客户端并关闭旧客户端；新客户端建连失败时保留旧客户端。
- 客户端建连失败后请求路径触发带冷却的重试，冷却期内不重复建连。
- limiter gateway 在客户端缺失时返回空 Mono，filter 按 fail-open 放行。
- 自动配置在容器中没有任何 Redisson bean 时也能完成装配。

集成验证覆盖：

- 本地通过 `docker compose up -d redis nacos` 启动 Redis 和 Nacos。
- 写入路由配置和治理配置。
- 连续请求命中限流阈值后返回 429。
- 多个网关实例共享同一个 Redis 限流状态。

## 非目标

第一版不做以下能力：

- 不做跨多个 Redis token bucket key 的严格原子扣减和回滚。
- 不支持一条路由绑定多个 `policyId`。
- 不支持复杂表达式匹配，例如按 method、header、IP 段组合匹配。
- 不提供独立 Admin UI，只复用已有 Nacos 配置链路。

# Redisson 限流策略设计文档

## 目标

在 `gateway-ratelimit` 模块中实现基于 Redisson 的分布式限流。路由只绑定限流策略，不感知具体限流规则；具体的服务总量、URL、用户等限流项统一放在 `aegis-governance.json` 中维护，并支持 Nacos 热更新。

## 背景与约束

- `gateway-server` 已依赖 `gateway-ratelimit`，但该模块目前只有 `build.gradle`，没有源码实现。
- `gateway-core` 已预留 `AegisFilterOrder.RATE_LIMIT = -100`，语义是在认证之后、灰度路由之前执行。
- `gateway-core` 已定义 429 业务错误码：`RATE_LIMIT_PATH`、`RATE_LIMIT_SERVICE`、`RATE_LIMIT_USER`。
- Nacos 是配置唯一来源；治理配置通过 `aegis-governance.json` 分发，各模块自行解析自己的配置段。
- Redisson 用于分布式令牌桶，多个网关实例共享 Redis 中的限流状态。

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
| `pathPattern` | `PATH` 规则使用的请求路径匹配模式，不包含 query string |
| `identityHeader` | `USER` 规则的用户标识请求头，默认 `X-User-Id` |

## 规则语义

### SERVICE

`SERVICE` 规则限制当前路由对应下游服务的总请求量。

- 适用范围：绑定该策略组的路由。
- key 维度：`policyId + ruleId + serviceId`。
- `serviceId` 优先从 `lb://service-name` 中提取；无法提取时退化为 routeId。

### PATH

`PATH` 规则限制某个请求 URL 模式。

- 只有当前请求 path 命中 `pathPattern` 时才生效。
- query string 不参与匹配，也不进入 Redis key。
- key 维度：`policyId + ruleId + pathPattern`。
- 不建议直接使用完整动态 URL 作为 key，例如 `/api/orders/123` 不应产生一个独立桶；应通过 `/api/orders/{id}` 或 `/api/orders/*` 这类模式归一。

### USER

`USER` 规则限制单个用户的请求频率。

- 用户标识从 `identityHeader` 指定的请求头读取，默认 `X-User-Id`。
- 请求头缺失时使用 `anonymous`，表示所有未识别用户共享一个匿名桶。
- key 维度：`policyId + ruleId + identity`。

## 执行流程

```text
请求进入网关
  -> SCG 匹配 Route
  -> RateLimitFilter 读取 Route metadata.rateLimit.policyId
  -> 从内存快照中找到对应 RateLimitPolicy
  -> 计算本次请求命中的 rules
  -> 按规则顺序逐个获取令牌
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

## Redisson 令牌桶设计

每条命中的规则对应一个 Redisson `RRateLimiter`。

Redis key 格式：

```text
aegis:ratelimit:{policyId}:{ruleId}:{dimension}:{dimensionValue}
```

示例：

```text
aegis:ratelimit:user-service-policy:user-service-total:service:user-service
aegis:ratelimit:user-service-policy:user-login-path:path:/api/users/login
aegis:ratelimit:user-service-policy:user-api-per-user:user:10001
```

令牌桶参数：

- `capacity` 映射为桶容量。
- `refillRate` 映射为每秒补充令牌数。
- 每个请求默认消耗 1 个令牌。

配置热更新时，filter 重新解析 `rateLimitPolicies` 并原子替换内存快照。实际 Redisson limiter 的参数需要按最新配置重新设置；如果 Redisson 对已存在 limiter 的参数更新有约束，使用规则配置指纹辅助判断是否需要重新初始化。

## 多规则扣令牌边界

第一版使用多个 Redisson `RRateLimiter` 组合实现多维度限流。需要明确一个边界：多个 Redis key 之间没有天然的跨 key 原子扣减语义。

因此第一版采用保守策略：

- 按命中的规则逐个 `tryAcquire(1)`。
- 任意规则失败立即返回 429。
- 已经成功扣减的前置规则不回滚。

这会带来一个可接受但需要记录的现象：如果前面的规则扣令牌成功，后面的规则失败，本次请求会被拒绝，但前面规则的令牌已经消耗。它的结果是限流略微更保守，不会导致超放。

为降低这种影响，规则执行顺序按更容易拒绝、更细粒度的规则优先：

```text
USER -> PATH -> SERVICE
```

如果后续需要严格的多桶原子扣减，需要改成自定义 Redis Lua token bucket，而不是直接组合多个 Redisson `RRateLimiter`。

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

## 模块设计

`gateway-ratelimit` 新增组件：

| 组件 | 职责 |
|---|---|
| `RateLimitAutoConfiguration` | 自动配置入口，注册 `RateLimitFilter` |
| `RateLimitFilter` | SCG `GlobalFilter`，执行限流判断和 429 响应 |
| `RateLimitPolicy` | 策略组模型，包含 `id` 和 `rules` |
| `RateLimitRule` | 单条规则模型 |
| `RateLimitType` | `SERVICE`、`PATH`、`USER` 枚举 |
| `RateLimitPolicyRepository` | 监听 Nacos governance 配置，维护策略快照 |
| `RateLimitKeyResolver` | 根据请求、路由和规则生成 Redis key |

`RateLimitFilter` 只依赖内存策略快照和 Redisson，不直接访问 Nacos。

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
- filter 顺序等于 `AegisFilterOrder.RATE_LIMIT`。
- governance 配置热更新后，新请求使用新策略快照。

集成验证覆盖：

- 本地通过 `docker compose up -d redis nacos` 启动 Redis 和 Nacos。
- 写入路由配置和治理配置。
- 连续请求命中限流阈值后返回 429。
- 多个网关实例共享同一个 Redis 限流状态。

## 非目标

第一版不做以下能力：

- 不做跨多个 Redisson limiter 的严格原子扣减和回滚。
- 不支持一条路由绑定多个 `policyId`。
- 不支持复杂表达式匹配，例如按 method、header、IP 段组合匹配。
- 不提供独立 Admin UI，只复用已有 Nacos 配置链路。

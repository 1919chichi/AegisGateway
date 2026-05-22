# AegisGateway 设计文档

**日期**: 2026-05-23  
**版本**: v1.0（第一版范围）

---

## 1. 项目定位

AegisGateway 是一个基于 Spring Cloud Gateway 的高性能微服务治理网关，充分利用 JDK 25 新特性（虚拟线程、结构化并发、Project Valhalla），以可执行 JAR / Docker 镜像形式交付，面向 Java 微服务场景。

**核心目标：**
- 在 Spring Cloud Gateway 路由引擎之上叠加完整的微服务治理能力
- 为现有 SCG 用户提供无感迁移路径，复用现有路由配置
- 以 Nacos 为唯一外部依赖，降低部署复杂度

**外部依赖：**
- Nacos：服务注册 + 配置中心
- Redis：通过 Redisson 接入，仅用于分布式限流

**不做：**
- 可观测性（Prometheus / OpenTelemetry）—— 列入后续版本
- Body 级别请求/响应变换 —— 列入后续版本

---

## 2. 整体架构

```
                    ┌─────────────────────────────────────────────────┐
                    │                 AegisGateway                    │
                    │                                                 │
请求入口 ──────────▶│  Auth → RateLimit → Gray → SCG 路由匹配         │──▶ 上游服务
                    │                ↓                                │
                    │  LoadBalance → CircuitBreaker → Retry           │
                    │                ↓                                │
                    │           Mirror (async VirtualThread)          │
                    └─────────────────┬───────────────────────────────┘
                                      │
                    ┌─────────────────▼───────────────────────────────┐
                    │                  Nacos                          │
                    │       服务注册 + 路由配置 + 治理策略              │
                    └─────────────────────────────────────────────────┘
```

**架构原则：**
- Spring Cloud Gateway 负责路由匹配和请求代理，不改动其核心
- 所有治理能力以 GlobalFilter 形式插入 Filter 链，按 Order 排序
- Nacos Config 是唯一配置来源，变更通过 Listener 热推到内存，无需重启
- 虚拟线程处理所有 I/O（Nacos 订阅、镜像请求）；Structured Concurrency 管理重试和超时生命周期

---

## 3. 模块结构（Gradle 多模块）

```
AegisGateway/
├── gateway-core/           # SCG 集成、Nacos 配置同步、Filter 链基础设施
├── gateway-ratelimit/      # 限流（本地令牌桶 + 分布式）
├── gateway-circuitbreaker/ # 熔断（Resilience4j）
├── gateway-loadbalancer/   # 负载均衡 + Nacos 多 namespace 服务发现
├── gateway-gray/           # 灰度发布 / 流量染色
├── gateway-auth/           # JWT / OAuth2 / API Key 认证鉴权
├── gateway-transform/      # Header 改写、路径重写、CORS
├── gateway-mirror/         # 流量镜像（虚拟线程 fire-and-forget）
├── gateway-admin/          # Admin REST API
└── gateway-server/         # 打包入口，输出可执行 JAR + Docker 镜像
```

**依赖原则：** 每个功能模块只依赖 `gateway-core`，功能模块之间不互相依赖，`gateway-server` 负责组装全部模块。

---

## 4. Filter 链顺序

| Order | Filter | 模块 | 说明 |
|-------|--------|------|------|
| -200 | AuthFilter | gateway-auth | 最先执行，未认证直接拒绝 |
| -100 | RateLimitFilter | gateway-ratelimit | 认证通过后限流 |
| -50  | GrayFilter | gateway-gray | 修改路由目标，流量染色 |
| 0    | SCG 内置路由匹配 | — | — |
| 100  | LoadBalancerFilter | gateway-loadbalancer | 从 Nacos 健康实例选择目标 |
| 200  | CircuitBreakerFilter | gateway-circuitbreaker | 熔断保护上游 |
| 300  | RetryFilter | gateway-ratelimit | 失败重试，联动熔断状态 |
| 400  | MirrorFilter | gateway-mirror | 异步镜像，不阻塞主链路 |

---

## 5. Nacos 配置模型

配置分三个 Data ID，所有变更通过 Nacos Listener 热更新到内存。

### 5.1 路由配置 `aegis-routes.json`

```json
{
  "routes": [
    {
      "id": "user-service",
      "uri": "lb://user-service",
      "predicates": ["Path=/api/user/**"],
      "filters": ["StripPrefix=1"],
      "metadata": {
        "gray": { "enabled": true },
        "rateLimit": { "ruleId": "user-service-limit" },
        "retry": { "maxAttempts": 3, "statusCodes": [502, 503] },
        "mirror": { "enabled": false }
      }
    }
  ]
}
```

### 5.2 治理策略 `aegis-governance.json`

```json
{
  "rateLimits": [
    {
      "id": "user-service-limit",
      "type": "PATH",
      "algorithm": "TOKEN_BUCKET",
      "capacity": 1000,
      "refillRate": 100
    }
  ],
  "circuitBreakers": [
    {
      "id": "user-service-cb",
      "failureRateThreshold": 50,
      "slowCallDurationThreshold": 2000
    }
  ],
  "grayRules": [
    {
      "serviceId": "user-service",
      "headerKey": "X-Version",
      "headerValue": "v2",
      "weight": 10
    }
  ]
}
```

### 5.3 全局配置 `aegis-global.json`

```json
{
  "cors": {
    "allowedOrigins": ["*"],
    "allowedMethods": ["GET", "POST", "PUT", "DELETE"]
  },
  "auth": {
    "jwtSecret": "...",
    "excludePaths": ["/api/public/**"]
  },
  "admin": {
    "apiKey": "..."
  }
}
```

---

## 6. JDK 25 核心用法

| 场景 | JDK 25 特性 | 实现方式 |
|------|------------|---------|
| Nacos 配置变更监听 | 虚拟线程 | 每个 Listener 跑在独立虚拟线程，不占用平台线程 |
| 流量镜像请求 | 虚拟线程 | `Thread.ofVirtual().start()` fire-and-forget，设置超时上限防泄漏 |
| 重试 + 超时协调 | Structured Concurrency | `StructuredTaskScope` 统一管理重试任务生命周期 |
| 路由上下文对象 | Value types (Valhalla) | 请求上下文用 value class，减少堆分配和 GC 压力 |

---

## 7. 各功能模块说明

### 7.1 限流（gateway-ratelimit）
- 支持维度：路径级、服务级、用户级
- 算法：Redisson `RRateLimiter`（令牌桶），天然支持分布式，多实例共享同一限流状态
- 每条限流规则对应一个 Redisson RRateLimiter 实例，key 由维度（路径/服务/用户）拼接
- 规则配置（capacity、refillRate）从 Nacos 热更新，变更时重建对应的 RRateLimiter

### 7.2 熔断（gateway-circuitbreaker）
- 基于 Resilience4j CircuitBreaker
- 支持失败率阈值、慢调用检测、半开探测
- 与重试联动：熔断打开时跳过重试直接快速失败

### 7.3 负载均衡（gateway-loadbalancer）
- 策略：加权轮询、最少连接、一致性哈希
- 服务实例列表来自 Nacos，天然过滤不健康实例（Nacos 心跳托管健康检查）
- 保留并迁移当前项目中获取 Nacos 不同 namespace 服务的能力（NacosServiceDiscoveryV2）

### 7.4 灰度发布（gateway-gray）
- 路由维度：请求 Header、用户标识、流量比例
- 染色标记透传到上游（X-Gray-Tag Header）
- 配置在 `aegis-governance.json` 中热更新

### 7.5 认证鉴权（gateway-auth）
- 支持：JWT 验证、OAuth2 Token Introspection、API Key
- 排除路径白名单支持通配符
- 插件化设计：实现 AuthProvider 接口可扩展新认证方式

### 7.6 请求/响应变换（gateway-transform）
- 复用 SCG 内置 Filter（AddRequestHeader、SetResponseHeader、RewritePath 等）
- 通过 Nacos 路由配置的 filters 字段声明，Admin API 管理
- CORS 通过 Spring WebFlux CorsWebFilter 实现，支持全局和路由级别配置

### 7.7 流量镜像（gateway-mirror）
- 自定义 GlobalFilter，fire-and-forget 异步发送镜像请求
- 虚拟线程执行，不阻塞主链路
- 支持采样比例（避免全量镜像压垮镜像端）
- 配置项：镜像目标地址、开关、采样率

### 7.8 重试（gateway-ratelimit 模块内）
- 基础重试：SCG RetryGatewayFilter（次数、可重试状态码白名单）
- 高级退避：Resilience4j Retry（指数退避 + jitter）
- Structured Concurrency 设置整个重试链路总超时上限
- 与熔断联动，半开状态下才允许重试

### 7.9 Admin REST API（gateway-admin）
- Spring WebFlux Controller 实现
- 写操作同步写回 Nacos Config，触发热更新
- 接口覆盖：路由 CRUD、治理策略管理、实例列表查询
- 自身通过 API Key 认证

---

## 8. 统一响应结构

所有 Admin REST API 及网关治理响应（限流、熔断等）统一使用以下结构：

```json
{
  "code": 200,
  "message": "success",
  "data": { },
  "timestamp": 1716451200000
}
```

- `code`：业务状态码，与 HTTP 状态码分离，支持细粒度错误区分
- `message`：可读描述
- `data`：响应数据，错误时统一为 `null`
- `timestamp`：服务器时间戳（毫秒），便于问题排查

**业务错误码规范：**

| code 范围 | 含义 |
|-----------|------|
| 200 | 成功 |
| 400xx | 请求参数错误（40001 参数缺失、40002 格式非法） |
| 401xx | 认证失败（40101 API Key 无效） |
| 403xx | 权限不足 |
| 404xx | 资源不存在（40401 路由不存在） |
| 429xx | 限流触发（42901 路径限流、42902 服务限流、42903 用户限流） |
| 500xx | 服务内部错误 |
| 503xx | 上游不可用（熔断触发） |

---

## 9. 第一版交付范围

| 功能 | 状态 |
|------|------|
| SCG 集成 + Nacos 路由热更新 | v1 |
| Nacos 多 namespace 服务发现 | v1 |
| 限流（路径/服务/用户级） | v1 |
| 熔断（Resilience4j） | v1 |
| 负载均衡（加权轮询/最少连接/一致性哈希） | v1 |
| 灰度发布 / 流量染色 | v1 |
| JWT / OAuth2 / API Key 认证 | v1 |
| Header 改写、路径重写、CORS | v1 |
| 重试（退避 + 熔断联动） | v1 |
| 流量镜像（虚拟线程） | v1 |
| Admin REST API | v1 |
| 可执行 JAR + Docker 镜像 | v1 |
| 可观测性（Prometheus / OpenTelemetry） | roadmap |
| Body 级请求/响应变换 | roadmap |
| 插件化架构（社区扩展） | roadmap |

---

## 10. 技术栈

| 组件 | 选型 |
|------|------|
| JDK | 25 |
| 构建工具 | Gradle |
| 路由引擎 | Spring Cloud Gateway |
| 服务注册/配置 | Nacos |
| 熔断 | Resilience4j |
| 限流 | Redisson RRateLimiter（令牌桶） |
| 并发模型 | 虚拟线程 + Structured Concurrency |
| 打包 | Docker + 可执行 JAR |

# AegisGateway

基于 **Spring Cloud Gateway（WebFlux）** 的响应式 API 网关，以 Nacos 作为唯一配置中心，运行于 **Java 25**，充分利用虚拟线程与 Structured Concurrency。

## 特性

- **动态路由**：所有路由配置存储在 Nacos，实时推送无需重启
- **JWT 认证**：可配置的路径排除规则
- **分布式限流**：基于 Redisson（Redis）的策略化限流，支持服务 / 路径 / 用户三个维度，Redis 故障时 fail-open
- **熔断**：基于 Resilience4j
- **负载均衡**：基于 Nacos 服务发现 + Spring Cloud LoadBalancer
- **灰度路由**：金丝雀流量分流
- **请求/响应转换**：Header 改写、Body 映射
- **流量镜像**：异步镜像流量至影子服务
- **Admin API**：通过 REST 接口管理 Nacos 配置
- **并行配置加载**：启动时使用 Java 25 Structured Concurrency 并行拉取所有配置，任一失败则整体失败

## 模块结构

```
aegis-gateway/
├── gateway-core          # 核心库：Nacos 配置同步、路由仓库、全局异常处理、共享模型
├── gateway-server        # 唯一可启动的 Spring Boot 应用，聚合所有模块
├── gateway-ratelimit     # 分布式限流（Redisson / Redis）
├── gateway-circuitbreaker# 熔断（Resilience4j）
├── gateway-loadbalancer  # 服务发现负载均衡（Nacos + Spring Cloud LoadBalancer）
├── gateway-gray          # 灰度 / 金丝雀路由
├── gateway-auth          # JWT 认证
├── gateway-transform     # 请求 / 响应转换
├── gateway-mirror        # 流量镜像
└── gateway-admin         # 配置管理 Admin REST API
```

## 技术栈

| 组件 | 版本 | 用途 |
|---|---|---|
| Java | 25（`--enable-preview`） | Records、虚拟线程、Structured Concurrency |
| Spring Boot | 4.0.6 | 应用框架 |
| Spring Cloud | 2025.1.1 | Gateway、LoadBalancer、CircuitBreaker |
| Spring Cloud Alibaba | 2025.1.0.0 | Nacos 服务发现 + 动态配置 |
| Nacos Client | 3.1.1 | 配置监听、服务注册 |
| Redisson | 4.4.0 | 分布式限流（Redis 客户端） |
| Resilience4j | 2.3.0 | 熔断 |
| Project Reactor | _随 Spring Boot BOM_ | 全链路响应式 |

## 快速开始

### 前置依赖

- JDK 25+
- Docker Compose（用于本地启动 Nacos 和 Redis）

### 启动本地基础设施

```bash
docker compose up -d nacos redis
```

默认端口：

| 服务 | 地址 |
|---|---|
| Nacos API | `127.0.0.1:8848` |
| Nacos 控制台 | `http://127.0.0.1:18080/` |
| Redis | `127.0.0.1:6379` |

### 构建

```bash
./gradlew :gateway-server:bootJar
```

### 启动

```bash
java --enable-preview -jar gateway-server/build/libs/gateway-server-*.jar
```

### 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `NACOS_SERVER_ADDR` | `127.0.0.1:8848` | Nacos 服务地址 |
| `NACOS_NAMESPACE` | _（空）_ | Nacos 命名空间 |
| `AEGIS_NACOS_GROUP` | `aegis` | 所有 Aegis 配置使用的 Nacos Group |

### Docker

```bash
# 先构建 JAR，再构建镜像
./gradlew :gateway-server:bootJar
docker build -t aegis-gateway .

# 运行
docker run -p 8080:8080 \
  -e NACOS_SERVER_ADDR=<nacos-host>:8848 \
  -e AEGIS_NACOS_GROUP=aegis \
  aegis-gateway
```

## Nacos 配置

网关从以下三个 Data ID 读取配置（Group 由 `AEGIS_NACOS_GROUP` 决定，默认 `aegis`）：

| Data ID | 格式 | 说明 |
|---|---|---|
| `aegis-routes.json` | JSON | 路由定义列表 |
| `aegis-governance.json` | JSON | 治理配置（限流、熔断等模块自行解析） |
| `aegis-global.json` | JSON | 全局配置：CORS、JWT 密钥、Admin API Key |

### 路由配置示例（`aegis-routes.json`）

```json
{
  "routes": [
    {
      "id": "user-service",
      "uri": "lb://user-service",
      "predicates": ["Path=/api/users/**"],
      "filters": ["StripPrefix=1"],
      "order": 0,
      "metadata": {}
    }
  ]
}
```

### 多 namespace 权重路由示例

同一个服务名可以拆成多条虚拟路由，通过 SCG `Weight` 控制 namespace 级流量比例。下面配置会把 `/api/users/**` 的流量按 80:20 分到 `dev` 和 `gray` namespace，两个 namespace 内仍调用同一个 `lb://user-service`。

```json
{
  "routes": [
    {
      "id": "user-service-dev",
      "uri": "lb://user-service",
      "predicates": [
        "Path=/api/users/**",
        "Weight=user-service,80"
      ],
      "filters": ["StripPrefix=1"],
      "order": 0,
      "metadata": {
        "discovery": {
          "namespace": "dev",
          "group": "DEFAULT_GROUP"
        }
      }
    },
    {
      "id": "user-service-gray",
      "uri": "lb://user-service",
      "predicates": [
        "Path=/api/users/**",
        "Weight=user-service,20"
      ],
      "filters": ["StripPrefix=1"],
      "order": 0,
      "metadata": {
        "discovery": {
          "namespace": "gray",
          "group": "DEFAULT_GROUP"
        }
      }
    }
  ]
}
```

`Weight` 只控制虚拟路由命中比例；命中某条虚拟路由后，`gateway-loadbalancer` 只读取该路由 `metadata.discovery.namespace` 指定 namespace 下的健康实例。

### 限流配置（`aegis-routes.json` + `aegis-governance.json`）

限流采用**策略绑定**模型：路由只通过 `metadata.rateLimit.policyId` 绑定一个策略组，具体限流规则统一放在 `aegis-governance.json` 中维护，支持 Nacos 热更新——调整限流参数不需要改动路由配置。

路由侧只需绑定策略 ID：

```json
{
  "routes": [
    {
      "id": "user-service",
      "uri": "lb://user-service",
      "predicates": ["Path=/api/users/**"],
      "filters": ["StripPrefix=1"],
      "metadata": {
        "rateLimit": {
          "policyId": "user-service-policy"
        }
      }
    }
  ]
}
```

治理配置侧定义 Redis 连接和策略规则（`aegis-governance.json`）：

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

每条规则是一个 Redis 令牌桶（Lua 脚本实现，多网关实例共享状态）：`capacity` 为桶容量（允许的突发量），`refillRate` 为每秒补充令牌数（近似稳定 QPS）。

三种限流维度：

| `type` | 语义 | 命中条件 |
|---|---|---|
| `SERVICE` | 限制下游服务总请求量 | 绑定该策略的路由的所有请求 |
| `PATH` | 限制单个 URL 模式（`pathPattern`，Spring `PathPattern` 语法） | 原始请求 path 匹配 `pathPattern` |
| `USER` | 限制单个用户请求频率（用户标识取自 `identityHeader`，默认 `X-User-Id`，缺失时共享匿名桶） | 所有请求，按用户区分令牌桶 |

放行采用 **AND 语义**：本次请求命中的所有规则都获取到令牌才放行，任一规则失败返回 `429` + 统一 `ApiResponse`，错误码按失败规则类型区分：

| 失败规则类型 | 错误码 |
|---|---|
| `PATH` | `42901` |
| `SERVICE` | `42902` |
| `USER` | `42903` |

**fail-open 与启动解耦**：限流是保护手段，不构成新的单点故障——路由未绑定策略、策略不存在、未配置 `rateLimitRedis`、Redis 不可用或扣令牌异常时一律放行。网关启动完全不依赖 Redis：Redis 客户端按治理配置惰性创建，没有限流策略时根本不建连；Redis 恢复后限流自动生效。

更多细节（key 设计、多规则扣减边界、热更新机制）见 [限流策略设计文档](docs/superpowers/specs/2026-06-11-redisson-rate-limit-policy-design.md)。

### 全局配置示例（`aegis-global.json`）

```json
{
  "cors": {
    "allowedOrigins": ["https://example.com"],
    "allowedMethods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
  },
  "auth": {
    "jwtSecret": "your-secret-key",
    "excludePaths": ["/api/public/**", "/actuator/health"]
  },
  "admin": {
    "apiKey": "your-admin-api-key"
  }
}
```

> **注意**：路由的增删必须通过 Admin API → Nacos，不支持直接调用 Spring Cloud Gateway 的路由仓库接口（`save`/`delete` 会抛 `UnsupportedOperationException`）。

## Filter 执行顺序

```
AUTH (-200) → RATE_LIMIT (-100) → GRAY (-50) → EXCEPTION_HANDLER (-2)
  → [SCG 内置 Filter]
  → CIRCUIT_BREAKER (10050) → RETRY (10300) → MIRROR (10400)
```

## 路由更新生命周期

```
Nacos 推送变更
  → NacosConfigSyncService 反序列化
  → AegisRouteDefinitionRepository 原子替换内存路由 Map
  → 发布 RefreshRoutesEvent
  → Spring Cloud Gateway 重新加载路由
```

## 测试

```bash
# 运行所有测试
./gradlew test

# 运行单个模块
./gradlew :gateway-core:test

# 运行单个测试类
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.route.AegisRouteDefinitionRepositoryTest"
```

## 新增功能模块

1. 创建模块目录，在 `build.gradle` 中添加 `implementation project(':gateway-core')`
2. 在 `settings.gradle` 中注册 `include 'gateway-<name>'`
3. 在 `gateway-server/build.gradle` 中添加 `implementation project(':gateway-<name>')`
4. 实现 `GlobalFilter` Bean，使用 `AegisFilterOrder` 中的顺序常量
5. 如需 Nacos 配置，通过 `NacosConfigSyncService.registerGovernanceListener()` 或 `registerGlobalListener()` 注册监听器

## 许可证

本项目仅供学习与参考。

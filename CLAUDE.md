# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 构建与测试命令

```bash
# 构建可运行的 JAR
./gradlew :gateway-server:bootJar

# 运行所有测试
./gradlew test

# 运行单个模块的测试
./gradlew :gateway-core:test

# 运行单个测试类
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.route.AegisRouteDefinitionRepositoryTest"

# 运行单个测试方法
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.route.AegisRouteDefinitionRepositoryTest.save_shouldReturnError_becauseNacosIsTheSingleSourceOfTruth"

# 启动网关（需要 Nacos 已运行）
java --enable-preview --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED -jar gateway-server/build/libs/gateway-server-*.jar
```

所有编译和测试任务都需要 `--enable-preview`（Java 25 预览特性），根 `build.gradle` 已统一配置，不要移除。

## 架构概览

AegisGateway 是基于 Spring Cloud Gateway（WebFlux）的响应式 API 网关，采用 Gradle 多模块结构。`gateway-server` 是唯一可启动的 Spring Boot 应用，其余模块均为库，由 `gateway-server` 统一组装。

### 模块职责

| 模块 | 职责 |
|---|---|
| `gateway-core` | Spring Boot autoconfigure 库：Nacos 配置同步、路由仓库、全局异常处理、共享模型、Filter 顺序常量 |
| `gateway-server` | Boot 应用入口，依赖所有其他模块 |
| `gateway-ratelimit` | 基于 Redisson（Redis）的分布式限流 |
| `gateway-circuitbreaker` | 基于 Resilience4j 的熔断 |
| `gateway-loadbalancer` | 基于 Nacos + Spring Cloud LoadBalancer 的服务发现负载均衡 |
| `gateway-gray` | 灰度/金丝雀路由 |
| `gateway-auth` | JWT 认证 |
| `gateway-transform` | 请求/响应转换 |
| `gateway-mirror` | 流量镜像 |
| `gateway-admin` | 配置管理的 Admin REST API |

### 配置唯一来源：Nacos

所有网关配置存储在 Nacos，而非 `application.yml`。监听三个配置文件：

- `aegis-routes.json` — 路由定义（对应 `AegisRoutesConfig`）
- `aegis-governance.json` — 治理配置，以原始 JSON 字符串分发给各功能模块
- `aegis-global.json` — 全局配置：CORS、JWT 密钥、Admin API Key（对应 `GlobalConfig`）

`NacosConfigSyncService` 在启动时使用 Java 25 **Structured Concurrency**（`StructuredTaskScope`）并行加载三个配置，任一加载失败则整体失败。每个配置项有独立的单线程虚拟线程 Executor，保证同一配置的变更串行有序投递。

路由的增删必须通过 Admin API → Nacos，`AegisRouteDefinitionRepository.save()` 和 `delete()` 会抛 `UnsupportedOperationException`，这是有意为之。

### 路由更新生命周期

1. Nacos 推送配置变更 → `NacosConfigSyncService` 反序列化
2. 调用已注册的监听器（如 `AegisRouteDefinitionRepository`）
3. Repository 原子替换内存路由 Map
4. 发布 `RefreshRoutesEvent` → SCG 重新加载路由

### Filter 执行顺序

统一定义在 `AegisFilterOrder`（数值越小越先执行）：

```
AUTH (-200) → RATE_LIMIT (-100) → GRAY (-50) → EXCEPTION_HANDLER (-2)
→ [SCG 内置 Filter]
→ CIRCUIT_BREAKER (10050) → RETRY (10300) → MIRROR (10400)
```

### 技术栈

- **Java 25** + `--enable-preview`：使用 Records、虚拟线程、Structured Concurrency
- **Spring Boot 4.0.6** / **Spring Cloud 2025.1.1** / **Spring Cloud Alibaba 2025.1.0.0**
- **Nacos**：服务发现 + 动态配置中心
- **Redisson**：分布式限流
- **Resilience4j**：熔断
- **Project Reactor**：所有 Filter 均为响应式（`GlobalFilter` 返回 `Mono<Void>`）

### 关键环境变量

| 变量 | 默认值 | 用途 |
|---|---|---|
| `NACOS_SERVER_ADDR` | `127.0.0.1:8848` | Nacos 服务地址 |
| `NACOS_NAMESPACE` | _(空)_ | Nacos 命名空间 |
| `AEGIS_NACOS_GROUP` | `aegis` | 所有 Aegis 配置使用的 Nacos Group |

### 新增功能模块步骤

1. 创建模块目录，`build.gradle` 中添加 `implementation project(':gateway-core')`
2. 在 `settings.gradle` 中添加 `include 'gateway-<name>'`
3. 在 `gateway-server/build.gradle` 中添加 `implementation project(':gateway-<name>')`
4. 实现 Spring `GlobalFilter` Bean，使用 `AegisFilterOrder` 中的顺序常量
5. 如需 Nacos 配置，通过 `NacosConfigSyncService.registerGovernanceListener()` 或 `registerGlobalListener()` 注册监听器

# gateway-core 模块指引

本文件仅说明 `gateway-core` 模块特有的信息，全局架构、构建命令、注释规范见根 `CLAUDE.md` 及 `docs/rules/`。

## 模块职责

`gateway-core` 是 Spring Boot **autoconfigure 库**（非可启动应用），向 `gateway-server` 及其他功能模块提供四类基础设施：

1. Nacos 配置同步（所有配置的唯一入口）
2. 以 Nacos 为唯一数据源的路由仓库
3. 全局异常处理与统一错误响应
4. 跨模块共享的模型、错误码、Filter 顺序常量

其他功能模块（ratelimit / circuitbreaker / loadbalancer / gray / auth / transform / mirror / admin）均 `implementation project(':gateway-core')`，并通过本模块暴露的 `NacosConfigSyncService` 监听配置、用 `AegisFilterOrder` 排序 Filter、用 `ApiResponse` / `AegisErrorCode` 输出统一响应。

依赖（见 `build.gradle`）：SCG WebFlux、Nacos discovery + config、Boot 4 的 Jackson（`tools.jackson.*`，注意不是 `com.fasterxml.jackson`）、autoconfigure-processor（compileOnly）。

## 关键类与作用

| 类 | 作用 |
|---|---|
| `config/AegisCoreAutoConfiguration` | 自动配置入口，注册下面三个核心 Bean |
| `nacos/NacosConfigSyncService` | 拉取 + 订阅 Nacos 三个配置，对外提供 getter 与 `registerXxxListener` |
| `nacos/NacosConfigKeys` | 三个 Data ID 常量 + 默认 Group `aegis` |
| `route/AegisRouteDefinitionRepository` | SCG `RouteDefinitionRepository` 实现，内存路由由 Nacos 驱动 |
| `exception/GlobalExceptionHandler` | `WebExceptionHandler` 兜底，异常 → `AegisErrorCode` |
| `exception/ApiErrorResponseWriter` | 把错误码写成统一 `ApiResponse` JSON 的共享工具 |
| `filter/AegisFilterOrder` | 全部 Filter 的 order 常量 |
| `model/ApiResponse` | 统一响应包装 record，`code` 是业务码不是 HTTP 状态码 |
| `model/AegisErrorCode` | 业务错误码枚举 |
| `model/AegisDiscoveryMetadata` | Nacos 发现坐标，兼作 exchange attribute 跨模块契约 |
| `model/config/*` | `AegisRoute` / `AegisRoutesConfig` / `GlobalConfig` 配置模型 |

自动配置通过 `src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` 注册，`@AutoConfiguration(before = GatewayAutoConfiguration.class)` 且仅在 REACTIVE Web 应用下生效。

## 重要设计约束 / 不变量

- **Nacos 是配置唯一来源**：所有配置走 `NacosConfigSyncService`，不读 `application.yml`。三个 Data ID：`aegis-routes.json`（→ `AegisRoutesConfig`）、`aegis-governance.json`（以原始 JSON 字符串透传给功能模块）、`aegis-global.json`（→ `GlobalConfig`）。Group 默认 `aegis`，可由 `aegis.gateway.nacos.group` 覆盖。

- **`save()` / `delete()` 故意抛 `UnsupportedOperationException`**：路由增删必须经 Admin API → Nacos，不允许绕过 Nacos 直改内存。修改此处会破坏单一数据源约束。

- **启动时并行加载、任一失败即整体失败**：`init()` 用 Java 25 Structured Concurrency（`StructuredTaskScope` + `awaitAllSuccessfulOrThrow`）并行加载三个配置，单个超时 5s。防止网关以不完整配置上线。

- **每个 Data ID 一个单线程虚拟线程 Executor**：保证同一配置的 Nacos 推送串行有序，避免乱序覆盖。`registerXxxListener` 在该 Executor 上回放当前快照——监听器**不应**注册后自行调 getter 拉初始值，否则与并发推送产生竞态。

- **路由更新生命周期**：监听器回调 → `applyRoutes` 原子替换 `routeStore`（`AtomicReference<Map>`）→ 发布 `RefreshRoutesEvent` 触发 SCG 重载。

- **Filter 顺序（`AegisFilterOrder`）**：数值越小越先；负值在 SCG 内置 Filter 前，正值在后。
  `AUTH(-200) → RATE_LIMIT(-100) → GRAY(-50) → EXCEPTION_HANDLER(-2)`，
  `CIRCUIT_BREAKER(10050) → LOAD_BALANCER(10100) → RETRY(10300) → MIRROR(10400)`。
  `LOAD_BALANCER` 必须在 SCG `ReactiveLoadBalancerClientFilter`（10150）之前。

- **错误码编码规则**：前三位对应 HTTP 状态（400xx→400，429xx→429…），后两位区分子场景。

- **限流 429 不能走 `GlobalExceptionHandler`**：该处理器无法区分限流维度（只能退化为 `RATE_LIMIT_PATH`），所以限流 Filter 必须自行用 `ApiErrorResponseWriter` 写出响应，不得抛异常。

- **统一错误出口**：所有 Filter 直接写错误响应都应经 `ApiErrorResponseWriter`，序列化失败时降级为手工拼接 JSON（依赖错误码 message 均为静态 ASCII，无需转义）。

- **`AegisDiscoveryMetadata.ATTR_KEY`** 是 gateway-gray 写入、gateway-loadbalancer 读取的 exchange attribute 契约，修改需同步两个模块。

## 测试

测试位于 `src/test/java`，覆盖自动配置、Nacos 同步、路由仓库、异常处理与模型反序列化。
- `NacosConfigSyncServiceTest`：Mockito mock `ConfigService`，用 Awaitility 等待虚拟线程 Executor 上的异步回放/更新。
- `AegisRouteDefinitionRepositoryTest`：用 `StepVerifier` 验证 `save`/`delete` 返回错误。
- `AegisCoreAutoConfigurationTest`：`ReactiveWebApplicationContextRunner` 验证 Bean 装配。

运行：

```bash
./gradlew :gateway-core:test
# 单个类
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.route.AegisRouteDefinitionRepositoryTest"
```

# gateway-gray 模块

> 本文件仅覆盖本模块特有信息，全局架构 / 构建命令 / 注释规范见根 `CLAUDE.md` 与 `docs/rules/`。

## 模块职责

灰度 / 金丝雀路由。根据请求头将命中规则的流量「重定向」到另一套服务发现坐标（命名空间 + 分组），从而在不改变路由 URI 的前提下，把特定用户群导向灰度实例。

本模块**不直接选择实例**，只负责匹配规则并把目标坐标写入 exchange attribute（key = `AegisDiscoveryMetadata.ATTR_KEY`），实际实例选择由 `gateway-loadbalancer` 在后续读取该 attribute 完成。

## 关键类

| 类 | 作用 |
|---|---|
| `GrayRoutingFilter` | 核心 `GlobalFilter`。持有两份 `AtomicReference` 快照：预编译规则列表与 `routeId → AegisDiscoveryMetadata` 映射。逐条匹配、命中即写 attribute 并直通。|
| `GrayMatcher` | 匹配策略接口，`matches(ServerWebExchange)`。新增匹配类型只需实现它，并在 `GrayRule.toMatcher()` 的 switch 中注册，无需改动 filter 主流程。|
| `HeaderGrayMatcher` | 当前唯一实现，精确匹配请求头 `key == value`（用 `getHeaders().getFirst(key)`）。|
| `GrayConfig` | `aegis-governance.json` 中灰度节点的反序列化模型，持有**有序** `List<GrayRule>`。|
| `GrayRule` | 单条规则：`type` / `key` / `value` / `targetRouteId`。`type` 当前仅支持 `"header"`，未知类型 `toMatcher()` 抛 `IllegalArgumentException`。|
| `GrayAutoConfiguration` | 自动配置入口，注册 `GrayRoutingFilter` Bean。|

## 配置来源与格式

规则来自 `aegis-governance.json` 下的某个 key（默认 `gray`，可由 `spring.aegis.gray.governance-key` 改为 `canary`、`staging` 等语义化名称）：

```json
{
  "gray": {
    "rules": [
      { "type": "header", "key": "X-User-Type", "value": "beta", "targetRouteId": "user-canary" }
    ]
  }
}
```

`targetRouteId` 引用的路由必须在 `aegis-routes.json` 的 `metadata.discovery` 中显式声明 `namespace` 和 `group`：

```json
{ "id": "user-canary", "metadata": { "discovery": { "namespace": "prod-canary", "group": "DEFAULT_GROUP" } } }
```

灰度坐标**不继承** `NacosDiscoveryProperties` 默认值——灰度的语义是「明确路由到不同命名空间」，未显式声明 `namespace`/`group` 的路由会被跳过（不进入 routeDiscoveryMap）。

## 匹配机制与关键行为

- **first-rule-wins**：按列表顺序逐条匹配，第一条命中的规则即生效并停止匹配（即便后续规则也能命中）。
- **header 精确匹配**：`HeaderGrayMatcher` 仅做 `equals` 精确比较，无正则 / 前缀语义。
- **热重载**：在构造时通过 `NacosConfigSyncService.registerGovernanceListener` 与 `registerRoutesListener` 注册监听器，并立即拉取一次当前快照。Nacos 推送变更时重建快照并 `AtomicReference` 原子替换；规则在更新时即预编译为 `CompiledRule(matcher, targetRouteId)`，避免热路径上 `toMatcher()` 的对象分配。
- **错误韧性**：
  - governance JSON 解析失败 → 记录 error 日志并**保留上一份规则**（不清空，不抛异常）。
  - 节点缺失 / 为 null → 规则清空为空列表。
  - 命中规则但 `targetRouteId` 在 routeDiscoveryMap 中找不到坐标 → 记录 warn 日志并直通（不写 attribute）。
- 以上任何分支都调用 `chain.filter(exchange)` 继续链路，灰度匹配永不阻断请求。

## Filter 注册与执行顺序

- 通过 `META-INF/spring/...AutoConfiguration.imports` 声明 `GrayAutoConfiguration` 自动装配。
- `@AutoConfiguration(after = AegisCoreAutoConfiguration.class)`，`@ConditionalOnClass(GlobalFilter.class)`，`@ConditionalOnBean(NacosConfigSyncService.class)`；Bean 上 `@ConditionalOnMissingBean`，允许用户自定义覆盖。
- `getOrder()` 返回 `AegisFilterOrder.GRAY`（-50），在 AUTH(-200)、RATE_LIMIT(-100) 之后、SCG 内置 filter 之前执行——须在负载均衡 filter 之前写好 discovery attribute。

## 与 gateway-core 的关系

- 依赖 `gateway-core`（`build.gradle` 中 `implementation project(':gateway-core')`），SCG 依赖为 `compileOnly`（由 `gateway-server` 提供运行时）。
- 复用 core 的：`NacosConfigSyncService`（配置同步与监听注册）、`AegisDiscoveryMetadata`（attribute / metadata key 常量与坐标模型）、`AegisRoute`/`AegisRoutesConfig`（路由模型）、`AegisFilterOrder.GRAY`（顺序常量）、`AegisCoreAutoConfiguration`（装配顺序锚点）。

## 测试

```bash
# 全部测试
./gradlew :gateway-gray:test

# 单个测试类
./gradlew :gateway-gray:test --tests "io.aegis.gateway.gray.filter.GrayRoutingFilterTest"
```

测试使用 `MockServerWebExchange` 构造请求、Mockito mock `NacosConfigSyncService`，并用 `ArgumentCaptor` 捕获已注册的监听器以模拟 Nacos 推送，从而覆盖热重载、first-rule-wins、无效 JSON 韧性等行为；`GrayAutoConfigurationTest` 用 `ApplicationContextRunner` 验证条件装配。

# gateway-gray 设计文档

## 目标

实现基于请求属性（当前支持请求头）的动态灰度路由，将命中规则的请求引导至指定 Nacos 命名空间的服务实例，规则和目标命名空间均通过 Nacos 动态配置，不需要重启网关。

## 背景与约束

- `gateway-loadbalancer` 已实现通过路由 `metadata.discovery.namespace/group` 的**静态**命名空间路由（适用于蓝绿/权重场景）
- `AegisFilterOrder.GRAY = -50` 已在 `gateway-core` 中预留，在 loadbalancer（10100）之前执行
- 灰度 filter 运行时路由已被 SCG 选中，可读取 `GATEWAY_ROUTE_ATTR`，也可向 `ServerWebExchange.attributes` 写入数据供下游 filter 读取
- 命名空间名称不在 gray 模块内重复定义，复用路由 metadata 中已有的 `discovery` 配置

## 变更范围

| 模块 | 变更 |
|---|---|
| `gateway-core` | 将 `AegisDiscoveryMetadata` 从 `gateway-loadbalancer` 移入，包路径改为 `io.aegis.gateway.core.model`；新增 `GrayConfig`、`GrayRule`、`GrayMatcher` 接口、`HeaderGrayMatcher` |
| `gateway-loadbalancer` | 更新 `AegisDiscoveryMetadata` import 路径；`NamespaceAwareNacosServiceInstanceListSupplier` 增加优先级读取逻辑 |
| `gateway-gray` | 新增 `GrayRoutingFilter`、`GrayAutoConfiguration` |

## 核心模型（gateway-core）

### GrayMatcher 接口

```java
public interface GrayMatcher {
    boolean matches(ServerWebExchange exchange);
}
```

扩展时只需实现此接口并在 `GrayRule.toMatcher()` 的 `switch` 中注册新 `type`，不改动 filter 主流程。

### GrayRule

```java
public record GrayRule(String type, String key, String value, String targetRouteId) {
    public GrayMatcher toMatcher() {
        return switch (type) {
            case "header" -> new HeaderGrayMatcher(key, value);
            default -> throw new IllegalArgumentException("Unknown gray rule type: " + type);
        };
    }
}
```

- `type`：匹配器类型，当前支持 `"header"`
- `key`：请求头名称
- `value`：期望值（精确匹配）
- `targetRouteId`：命中后使用哪条路由的 `metadata.discovery` 作为服务发现坐标

### GrayConfig

```java
public record GrayConfig(List<GrayRule> rules) {
    public GrayConfig {
        rules = rules == null ? List.of() : List.copyOf(rules);
    }
}
```

### AegisDiscoveryMetadata（从 gateway-loadbalancer 移入）

从 `io.aegis.gateway.loadbalancer.discovery` 移至 `io.aegis.gateway.core.model`，新增 exchange attribute key 常量：

```java
public record AegisDiscoveryMetadata(String namespace, String group) {
    public static final String ATTR_KEY = AegisDiscoveryMetadata.class.getName();
    // 其余字段和逻辑不变
}
```

## 路由 metadata 快照

`GrayRoutingFilter` 同时通过 `NacosConfigSyncService.registerRoutesListener()` 监听路由变更，将所有路由的 `metadata.discovery` 构建成 `Map<String, AegisDiscoveryMetadata>`（routeId → 服务发现坐标），存入 `AtomicReference`。

这样 filter 在热路径上只做内存 Map 查找，不触发响应式链路，也不依赖 `RouteDefinitionRepository` 的 `Flux` 接口。路由配置变更时，监听器原子更新 Map，与规则快照的更新互相独立。

## 数据流

```
请求进入
  └─ GrayRoutingFilter (order = -50)
       ├─ 读 AtomicReference<GrayConfig> 取规则快照
       ├─ 逐条：rule.toMatcher().matches(exchange)
       ├─ 命中
       │    ├─ 从 AtomicReference<Map<routeId, AegisDiscoveryMetadata>> 查 targetRouteId
       │    ├─ 找到 → exchange.attributes.put(AegisDiscoveryMetadata.ATTR_KEY, metadata)
       │    └─ 找不到 → 记录 warn 日志，pass through
       └─ 未命中 → pass through，不写 exchange attribute

  └─ NamespaceAwareNacosServiceInstanceListSupplier (order = 10100)
       ├─ 优先级 1：exchange attributes 中的 AegisDiscoveryMetadata（灰度动态覆盖）
       ├─ 优先级 2：Route.metadata.discovery（静态配置）
       └─ 优先级 3：NacosDiscoveryProperties 全局默认值
```

## 配置

### aegis-governance.json

配置节点 key 由应用属性 `spring.aegis.gray.governance-key` 指定，默认 `"gray"`，允许用户使用语义化的名称（如 `"canary"`、`"staging"`）。

```json
{
  "gray": {
    "rules": [
      {
        "type": "header",
        "key": "X-User-Type",
        "value": "beta",
        "targetRouteId": "user-service-canary"
      }
    ]
  }
}
```

### aegis-routes.json（无需改动，示例）

```json
{
  "routes": [
    {
      "id": "user-service-canary",
      "uri": "lb://user-service",
      "predicates": ["Path=/api/users/**"],
      "metadata": {
        "discovery": { "namespace": "prod-canary", "group": "DEFAULT_GROUP" }
      }
    }
  ]
}
```

`targetRouteId` 引用已有路由，namespace 只在路由配置中定义一次，gray 规则不重复。

## 扩展性

新增匹配类型（如 JWT claim）步骤：
1. 实现 `GrayMatcher` 接口（如 `JwtClaimGrayMatcher`），放入 `gateway-core`
2. 在 `GrayRule.toMatcher()` 的 `switch` 中添加 `case "jwt"`
3. 配置中使用 `"type": "jwt"`

filter 主流程、loadbalancer、配置模型均无需修改。

## 未实现（超出当前范围）

- 非精确匹配（正则、前缀）：当前 `HeaderGrayMatcher` 只做 `equals` 匹配
- JWT claim 匹配器：接口已预留，实现留待后续
- 灰度请求下游 header 透传（如 `X-Gray: true`）：如需后端感知灰度链路，后续在 filter 中单独添加

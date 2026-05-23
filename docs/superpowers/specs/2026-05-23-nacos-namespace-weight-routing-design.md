# Nacos Namespace Weight Routing Design

**日期**: 2026-05-23
**状态**: ready-for-review

## 1. 目标

实现同一个服务名在多个 Nacos namespace 之间按比例分流，并继续复用 Spring Cloud Gateway 的内置 `Weight` 路由谓词和 `ReactiveLoadBalancerClientFilter`。

核心语义：

- `Weight` 控制 namespace 级流量比例。
- `lb://service-name` 保持不变。
- `gateway-loadbalancer` 根据已匹配路由的 metadata 选择 Nacos namespace，并只返回该 namespace 下的健康实例。
- namespace 内的实例选择继续交给 Spring Cloud LoadBalancer / Nacos LoadBalancer。

## 2. 非目标

- 不实现自定义 `GlobalFilter` 替代 SCG 内置负载均衡。
- 不改写 `GATEWAY_REQUEST_URL_ATTR`。
- 不把多个 namespace 的实例合并后再做实例级权重。
- 不用 Nacos 实例 `nacos.weight` 表达 namespace 间比例；该权重只适合 namespace 内实例选择。

## 3. 配置模型

同一个服务名按 namespace 拆成多条虚拟路由。每条虚拟路由使用相同的 `Path`、相同的 `lb://service-name`，并在同一个 `Weight` group 下声明权重。

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

字段规则：

- `metadata.discovery.namespace`：目标 Nacos namespace ID。为空时使用 `spring.cloud.nacos.discovery.namespace`。
- `metadata.discovery.group`：目标 Nacos group。为空时使用 `spring.cloud.nacos.discovery.group`。
- `Weight` group 建议使用服务名，例如 `user-service`，让同一服务的多个 namespace 路由互相竞争。
- 同一 `Weight` group 下的路由应使用相同的业务匹配条件，避免某些请求只落到部分候选路由。

## 4. 请求数据流

1. SCG `RoutePredicateHandlerMapping` 遍历路由。
2. `WeightCalculatorWebFilter` 按 group 计算本次请求选中的 routeId。
3. `WeightRoutePredicateFactory` 只让被选中的虚拟路由通过谓词匹配。
4. 匹配成功后，SCG 将 route 放入 exchange attribute。
5. `ReactiveLoadBalancerClientFilter` 处理 `lb://user-service`，构造 LoadBalancer request。
6. `gateway-loadbalancer` 的自定义 `ServiceInstanceListSupplier` 从 request attributes 中读取已匹配 route 的 `metadata.discovery`。
7. `NacosNamingServiceRegistry` 根据 namespace 缓存或创建对应 `NamingService`。
8. supplier 调用 `selectInstances(serviceId, group, true)` 获取健康实例。
9. 返回的 `ServiceInstance` metadata 补充来源信息：
   - `aegis.nacos.namespace`
   - `aegis.nacos.group`
10. Spring Cloud LoadBalancer / Nacos LoadBalancer 从该 namespace 的实例列表中选择最终实例。

## 5. 组件设计

### 5.1 `AegisDiscoveryMetadata`

职责：从 `Route.getMetadata()` 中解析服务发现配置。

字段：

- `namespace`
- `group`

解析失败或类型不匹配时不让请求失败，记录 warn 并回退到默认 discovery 配置。

### 5.2 `NacosNamingServiceRegistry`

职责：按 namespace 复用 `NamingService`。

输入：

- 当前应用的 `NacosDiscoveryProperties`
- 目标 namespace

行为：

- 复制默认 Nacos discovery properties。
- 覆盖 `PropertyKeyConst.NAMESPACE`。
- 调用 `NamingFactory.createNamingService(properties)`。
- 按 namespace 缓存 `NamingService`。
- 应用关闭时依次调用 `NamingService.shutDown()`。

### 5.3 `NamespaceAwareNacosServiceInstanceListSupplier`

职责：作为 Spring Cloud LoadBalancer 的实例列表供应器。

行为：

- `getServiceId()` 返回当前 `lb://` 的 serviceId。
- `get(Request request)` 从 LoadBalancer request 中取 SCG route metadata。
- 根据 namespace/group 查询 Nacos 健康实例。
- 将 Nacos `Instance` 转成 `NacosServiceInstance`，保留 `nacos.weight`、`nacos.cluster` 等原始 metadata。
- 查询异常时返回空列表，由 SCG 现有逻辑返回上游不可用。

## 6. Auto Configuration

在 `gateway-loadbalancer` 中新增 auto configuration，并通过 `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` 暴露。

需要注册：

- `NacosNamingServiceRegistry`
- 默认 `ServiceInstanceListSupplier` 定制，覆盖 Spring Cloud Alibaba 默认 supplier

约束：

- 不覆盖 `ReactiveLoadBalancerClientFilter`。
- 不禁用 `LoadBalancerNacosAutoConfiguration`。
- 只替换实例列表来源，不替换最终选择算法。

## 7. 失败策略

- 指定 namespace 不存在或连接失败：当前虚拟路由返回空实例列表，最终由 SCG 返回找不到上游实例。
- 某个 namespace 无健康实例：不自动降级到其他 namespace，否则会破坏 SCG Weight 选中的比例语义。
- metadata 缺失：使用默认 discovery namespace/group，兼容普通 `lb://service` 路由。
- metadata 类型非法：记录 warn 并使用默认 discovery namespace/group。

## 8. 测试策略

单元测试：

- metadata 中 namespace/group 能被正确解析。
- metadata 缺失时回退默认 namespace/group。
- `NacosNamingServiceRegistry` 对相同 namespace 复用同一个 `NamingService`。
- supplier 只查询已匹配 route metadata 指定的 namespace。
- Nacos 查询异常时返回空列表。

集成测试：

- 两条相同 Path 路由配置 `Weight=user-service,80` 和 `Weight=user-service,20`，确认 SCG 只会匹配其中一条虚拟路由。
- 选中 `user-service-dev` 时只调用 dev namespace 的 `NamingService`。
- 选中 `user-service-gray` 时只调用 gray namespace 的 `NamingService`。

## 9. 运维与排查

每次实例查询日志至少包含：

- routeId
- serviceId
- namespace
- group
- instance count

返回给 LoadBalancer 的实例 metadata 中保留 `aegis.nacos.namespace`，方便通过调试日志或后续观测能力确认请求实际进入哪个 namespace。

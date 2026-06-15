# gateway-loadbalancer

基于 **Nacos + Spring Cloud LoadBalancer** 的服务发现与负载均衡模块。核心特色是 **命名空间（namespace）感知** 的实例发现：同一 `serviceId` 可根据请求上下文路由到不同 Nacos 命名空间/分组下的实例，从而支撑灰度、多环境隔离等场景。

> 全局架构、Filter 顺序总览、环境变量、技术栈见根目录 `CLAUDE.md` 与 `docs/rules/`，本文件只记录本模块特有信息。

## 关键类

| 类 | 作用 |
|---|---|
| `config/AegisLoadBalancerAutoConfiguration` | 模块自动配置入口（已在 `AutoConfiguration.imports` 注册）。注册 `NacosNamingServiceRegistry` Bean；通过 `@LoadBalancerClients(defaultConfiguration = AegisNamespaceLoadBalancerClientConfiguration.class)` 把命名空间感知配置设为**所有** LB 客户端的默认配置 |
| `config/AegisNamespaceLoadBalancerClientConfiguration` | 每个 LB 客户端（每个 serviceId）的配置类，从 `LoadBalancerClientFactory.PROPERTY_NAME` 取出 serviceId，构造 `NamespaceAwareNacosServiceInstanceListSupplier` 替换默认 Supplier |
| `discovery/NamespaceAwareNacosServiceInstanceListSupplier` | 核心 `ServiceInstanceListSupplier`，负责按命名空间/分组查询 Nacos 实例 |
| `discovery/NacosNamingServiceRegistry` | 按命名空间缓存并复用 `NamingService`（创建成本高、含长连接），`@PreDestroy` 时统一关闭 |

## 命名空间感知发现机制

`NamespaceAwareNacosServiceInstanceListSupplier.loadInstances()` 按以下**优先级**确定目标 namespace + group（坐标封装为 `gateway-core` 的 `AegisDiscoveryMetadata` record）：

1. **Exchange attribute**（key = `AegisDiscoveryMetadata.ATTR_KEY`）—— 由 `gateway-gray` 的灰度 Filter 在运行时写入，优先级最高，可动态覆盖路由静态配置。
2. **SCG Route metadata**（`metadata.discovery.{namespace,group}`）—— 在 `aegis-routes.json` 中静态声明。
3. **应用级默认**（`NacosDiscoveryProperties`）—— 兜底。

得到坐标后，向 `NacosNamingServiceRegistry` 请求对应命名空间的 `NamingService`，调用 `selectInstances(serviceId, group, true)`（仅取健康实例）。

行为约束：
- Nacos SDK 查询是**阻塞调用**，订阅运行在 `Schedulers.boundedElastic()`，不阻塞 Reactor 事件循环。
- 查询失败时**降级返回空列表**（不抛异常），由上层熔断/重试机制处理。
- 每个返回的 `ServiceInstance` 会被注入两个 metadata：`aegis.nacos.namespace`、`aegis.nacos.group`（常量定义在该类中），供下游 Filter / 日志使用。
- Request 上下文解包统一走 `extractAttributes()`，仅当 `request.getContext()` 为 `RequestDataContext` 且 `RequestData` 非空时才能拿到 exchange attributes，否则视为缺失走回退链。

## 与 SCG 内置 LB filter 的协作

本模块**不实现自定义 GlobalFilter**，而是替换 Spring Cloud LoadBalancer 的 `ServiceInstanceListSupplier`。实际的 `lb://` URI 解析仍由 SCG 内置的 `ReactiveLoadBalancerClientFilter`（order **10150**）完成。

注意 `gateway-core` 的 `AegisFilterOrder.LOAD_BALANCER = 10100`（在 10150 之前执行）属于另一处机制；本模块本身不绑定该顺序常量，而是通过 Supplier 注入点参与 SCG 默认负载均衡流程。

## 与 gateway-core 的关系

- 依赖 `gateway-core`，复用其 `model/AegisDiscoveryMetadata`（namespace+group 坐标 + exchange attribute 跨模块契约，含默认值归一化逻辑，如空 group → `DEFAULT_GROUP`）。
- 与 `gateway-gray` 是**生产者/消费者**关系：gray 写入 attribute，本模块读取并据此切换发现坐标。

## 依赖（build.gradle）

- `spring-cloud-starter-loadbalancer`、`spring-cloud-starter-alibaba-nacos-discovery`、`caffeine`
- `spring-cloud-starter-gateway-server-webflux` 为 `compileOnly`（运行期由 gateway-server 提供）

## 自动配置条件

`@ConditionalOnClass({NamingService, ServiceInstanceListSupplier})` + `@ConditionalOnBean(NacosDiscoveryProperties)`，并 `@AutoConfiguration(after = LoadBalancerNacosAutoConfiguration.class)` 确保 Nacos Bean 就绪后再装配。

## 测试

```bash
# 全模块测试
./gradlew :gateway-loadbalancer:test

# 单个测试类
./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.discovery.NamespaceAwareNacosServiceInstanceListSupplierTest"
```

测试不依赖真实 Nacos：
- `NacosNamingServiceRegistry` 提供包级构造器注入 `NamingServiceFactory` mock（验证按命名空间创建/复用、shutdown 关闭）。
- `NamespaceAwareNacosServiceInstanceListSupplierTest` 覆盖：路由 metadata 取 namespace、缺失时回退默认、查询失败返回空列表、exchange attribute 优先于路由 metadata。
- `AegisLoadBalancerAutoConfigurationTest` 用 `ApplicationContextRunner` 验证 Bean 注册与 Supplier 类型（mock 掉 `NacosServiceManager` / `InetUtils` 等 Nacos 依赖）。

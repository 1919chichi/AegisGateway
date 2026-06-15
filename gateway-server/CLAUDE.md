# gateway-server 模块

本文件聚焦 `gateway-server` 模块的特有信息，全局架构与注释规范见根 `/CLAUDE.md` 及 `docs/rules/`。

## 模块职责

`gateway-server` 是整个项目中**唯一可启动的 Spring Boot 应用**，其余 `gateway-*` 模块均为库。本模块的职责是把所有功能库组装成一个可运行的网关进程：通过 `build.gradle` 依赖各库模块，由各库的 Spring Boot autoconfigure 机制自动装配 Filter、监听器等 Bean。本模块自身几乎不写业务逻辑。

## 启动类

`src/main/java/io/aegis/gateway/server/AegisGatewayApplication.java`：标准 `@SpringBootApplication` 入口，`main` 仅调用 `SpringApplication.run(...)`。不含自定义 Bean 或配置类——所有功能由依赖库的 auto-configuration 提供。

## 依赖的 gateway-* 模块

`build.gradle` 中以 `implementation project(...)` 方式聚合全部库模块：

`gateway-core`、`gateway-ratelimit`、`gateway-circuitbreaker`、`gateway-loadbalancer`、`gateway-gray`、`gateway-auth`、`gateway-transform`、`gateway-mirror`、`gateway-admin`。

另引入：

- `spring-boot-starter-webflux`：响应式 Web 运行时（SCG 基于 WebFlux）。
- `netty-resolver-dns-native-macos`（classifier `osx-aarch_64`，`runtimeOnly`）：仅供 macOS ARM 开发环境使用的 Netty 原生 DNS 解析器。

## application.yml 关键配置

本模块的 `application.yml` 只保留启动所必需的引导配置（**业务配置以 Nacos 为唯一来源**，见根 CLAUDE.md）：

| 配置 | 含义 |
|---|---|
| `server.port: 8080` | 网关监听端口 |
| `spring.application.name: aegis-gateway` | 应用名 / Nacos 服务注册名 |
| `spring.cloud.nacos.discovery` | Nacos 服务发现：`server-addr` / `namespace` / `group` 均由环境变量注入 |
| `spring.cloud.nacos.config` | Nacos 配置中心：`file-extension: json`；`import-check.enabled: false` 关闭 `spring.config.import` 强制校验，因配置在运行时由 `NacosConfigSyncService` 主动拉取而非 import 引导 |
| `aegis.gateway.nacos.group` | Aegis 自定义配置同步使用的 Nacos Group（绑定到 core 模块的配置属性） |
| `logging.level.io.aegis.gateway: DEBUG` | 网关自身代码开启 DEBUG 日志 |

## 关键环境变量

| 变量 | 默认值 | 用途 |
|---|---|---|
| `NACOS_SERVER_ADDR` | `127.0.0.1:8848` | Nacos 服务地址（discovery 与 config 共用） |
| `NACOS_NAMESPACE` | _(空)_ | Nacos 命名空间 |
| `AEGIS_NACOS_GROUP` | `aegis` | discovery、config 及 `aegis.gateway.nacos.group` 共用的 Nacos Group |

## 构建与启动

```bash
# 构建可运行 JAR
./gradlew :gateway-server:bootJar

# 启动（需 Nacos 已运行；JVM 参数不可省略）
java --enable-preview --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
  -jar gateway-server/build/libs/gateway-server-*.jar

# 开发期直接运行（bootRun 已在 build.gradle 中预置上述 JVM 参数）
./gradlew :gateway-server:bootRun
```

注意：

- 启动前必须有可用的 Nacos，否则配置/服务发现引导失败导致应用无法启动。
- `--enable-preview` 等三个 JVM 参数为 Java 25 预览特性所需，`bootRun` 任务已在本模块 `build.gradle` 中显式配置；以 `java -jar` 方式运行时需手动带上。

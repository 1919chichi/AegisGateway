# gateway-gray Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现基于请求头的动态灰度路由，命中规则的请求被引导至目标路由配置的 Nacos 命名空间实例，规则和目标均通过 Nacos 热更新。

**Architecture:** `GrayRoutingFilter`（order = -50）在每个请求上匹配 `GrayConfig` 规则，命中后将目标路由的 `AegisDiscoveryMetadata`（namespace + group）写入 exchange attribute；`NamespaceAwareNacosServiceInstanceListSupplier` 优先读取该属性，实现动态命名空间覆盖。`AegisDiscoveryMetadata` 从 `gateway-loadbalancer` 下沉至 `gateway-core` 成为跨模块共享契约。

**Tech Stack:** Java 25, Spring Boot 4.0.6, Spring Cloud Gateway 5.0.1, Spring Cloud Alibaba Nacos 2025.1.0.0, JUnit 5, Mockito, Reactor Test, tools.jackson.databind.

---

## File Structure

**gateway-core（移动现有文件）**
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/model/AegisDiscoveryMetadata.java` — 从 loadbalancer 移入，新增 `ATTR_KEY`
- Create: `gateway-core/src/test/java/io/aegis/gateway/core/model/AegisDiscoveryMetadataTest.java` — 从 loadbalancer 移入
- Delete: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadata.java`
- Delete: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadataTest.java`

**gateway-loadbalancer（更新引用）**
- Modify: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplier.java` — 更新 import；`loadInstances()` 增加 exchange attribute 优先级
- Modify: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplierTest.java` — 更新 import；新增 exchange attribute 优先级测试

**gateway-gray（全部新建）**
- Modify: `gateway-gray/build.gradle` — 添加 gateway 依赖
- Create: `gateway-gray/src/main/java/io/aegis/gateway/gray/matcher/GrayMatcher.java`
- Create: `gateway-gray/src/main/java/io/aegis/gateway/gray/matcher/HeaderGrayMatcher.java`
- Create: `gateway-gray/src/main/java/io/aegis/gateway/gray/model/GrayRule.java`
- Create: `gateway-gray/src/main/java/io/aegis/gateway/gray/model/GrayConfig.java`
- Create: `gateway-gray/src/main/java/io/aegis/gateway/gray/filter/GrayRoutingFilter.java`
- Create: `gateway-gray/src/main/java/io/aegis/gateway/gray/config/GrayAutoConfiguration.java`
- Create: `gateway-gray/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Create: `gateway-gray/src/test/java/io/aegis/gateway/gray/matcher/HeaderGrayMatcherTest.java`
- Create: `gateway-gray/src/test/java/io/aegis/gateway/gray/model/GrayRuleTest.java`
- Create: `gateway-gray/src/test/java/io/aegis/gateway/gray/filter/GrayRoutingFilterTest.java`
- Create: `gateway-gray/src/test/java/io/aegis/gateway/gray/config/GrayAutoConfigurationTest.java`

---

## Pre-flight

- [ ] **Step 1: 确认编译基线**

```bash
./gradlew :gateway-core:test :gateway-loadbalancer:test
```

Expected: `BUILD SUCCESSFUL`

---

## Task 1: 将 AegisDiscoveryMetadata 下沉到 gateway-core

**Files:**
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/model/AegisDiscoveryMetadata.java`
- Create: `gateway-core/src/test/java/io/aegis/gateway/core/model/AegisDiscoveryMetadataTest.java`
- Delete: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadata.java`
- Delete: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadataTest.java`
- Modify: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplier.java`
- Modify: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplierTest.java`

- [ ] **Step 1: 在 gateway-core 新建 AegisDiscoveryMetadata**

创建 `gateway-core/src/main/java/io/aegis/gateway/core/model/AegisDiscoveryMetadata.java`：

```java
package io.aegis.gateway.core.model;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Objects;

/**
 * Nacos 服务发现坐标（命名空间 + 分组），同时作为 exchange attribute 的跨模块共享契约。
 * <p>
 * 路由可在 {@code aegis-routes.json} 的 {@code metadata.discovery} 中静态声明坐标；
 * {@code gateway-gray} 在运行时将命中灰度规则的请求坐标写入 exchange attribute（key = {@link #ATTR_KEY}），
 * 供 {@code gateway-loadbalancer} 动态覆盖静态路由配置。
 */
public record AegisDiscoveryMetadata(String namespace, String group) {

    private static final Logger log = LoggerFactory.getLogger(AegisDiscoveryMetadata.class);

    /**
     * 写入 {@code ServerWebExchange.attributes} 时使用的 key，用类名保证唯一且 IDE 可追踪。
     */
    public static final String ATTR_KEY = AegisDiscoveryMetadata.class.getName();
    public static final String DISCOVERY_METADATA_KEY = "discovery";
    public static final String NAMESPACE_KEY = "namespace";
    public static final String GROUP_KEY = "group";
    /** Nacos 默认分组名，未指定 group 时使用。 */
    public static final String DEFAULT_GROUP = "DEFAULT_GROUP";

    public AegisDiscoveryMetadata {
        namespace = Objects.toString(namespace, "");
        group = StringUtils.hasText(group) ? group : DEFAULT_GROUP;
    }

    public static AegisDiscoveryMetadata from(Route route, NacosDiscoveryProperties defaults) {
        AegisDiscoveryMetadata fallback = fromDefaults(defaults);
        if (route == null) {
            return fallback;
        }

        Object discovery = route.getMetadata().get(DISCOVERY_METADATA_KEY);
        if (!(discovery instanceof Map<?, ?> discoveryMap)) {
            if (discovery != null) {
                log.warn("Route {} discovery metadata must be a map, actual={}", route.getId(), discovery.getClass().getName());
            }
            return fallback;
        }

        return new AegisDiscoveryMetadata(
                stringValue(route, discoveryMap, NAMESPACE_KEY, fallback.namespace()),
                stringValue(route, discoveryMap, GROUP_KEY, fallback.group()));
    }

    public static AegisDiscoveryMetadata fromDefaults(NacosDiscoveryProperties defaults) {
        return new AegisDiscoveryMetadata(defaults.getNamespace(), defaults.getGroup());
    }

    private static String stringValue(Route route, Map<?, ?> map, String key, String fallback) {
        Object value = map.get(key);
        if (value instanceof String text && StringUtils.hasText(text)) {
            return text;
        }
        if (value != null) {
            log.warn("Route {} discovery metadata key {} must be a non-blank string, actual={}",
                    route.getId(), key, value.getClass().getName());
        }
        return fallback;
    }
}
```

- [ ] **Step 2: 在 gateway-core 新建 AegisDiscoveryMetadataTest**

创建 `gateway-core/src/test/java/io/aegis/gateway/core/model/AegisDiscoveryMetadataTest.java`：

```java
package io.aegis.gateway.core.model;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.route.Route;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AegisDiscoveryMetadataTest {

    @Test
    void from_shouldReadNamespaceAndGroupFromRouteMetadata() {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        Route route = routeWithDiscovery(Map.of("namespace", "gray", "group", "GROUP_A"));

        AegisDiscoveryMetadata metadata = AegisDiscoveryMetadata.from(route, defaults);

        assertThat(metadata.namespace()).isEqualTo("gray");
        assertThat(metadata.group()).isEqualTo("GROUP_A");
    }

    @Test
    void from_shouldFallbackToDefaultsWhenDiscoveryMetadataIsMissing() {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        Route route = Route.async()
                .id("user-service-default")
                .uri("lb://user-service")
                .predicate(exchange -> true)
                .build();

        AegisDiscoveryMetadata metadata = AegisDiscoveryMetadata.from(route, defaults);

        assertThat(metadata.namespace()).isEqualTo("public");
        assertThat(metadata.group()).isEqualTo("DEFAULT_GROUP");
    }

    @Test
    void from_shouldFallbackToDefaultsWhenDiscoveryMetadataHasInvalidTypes() {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        Route route = routeWithDiscovery(Map.of("namespace", 123, "group", true));

        AegisDiscoveryMetadata metadata = AegisDiscoveryMetadata.from(route, defaults);

        assertThat(metadata.namespace()).isEqualTo("public");
        assertThat(metadata.group()).isEqualTo("DEFAULT_GROUP");
    }

    @Test
    void from_shouldUseDefaultGroupWhenDefaultGroupIsBlank() {
        NacosDiscoveryProperties defaults = defaults("", "");
        Route route = routeWithDiscovery(Map.of("namespace", "gray"));

        AegisDiscoveryMetadata metadata = AegisDiscoveryMetadata.from(route, defaults);

        assertThat(metadata.namespace()).isEqualTo("gray");
        assertThat(metadata.group()).isEqualTo("DEFAULT_GROUP");
    }

    @Test
    void attrKey_shouldBeClassNameForIdeTraceability() {
        assertThat(AegisDiscoveryMetadata.ATTR_KEY)
                .isEqualTo("io.aegis.gateway.core.model.AegisDiscoveryMetadata");
    }

    private static NacosDiscoveryProperties defaults(String namespace, String group) {
        NacosDiscoveryProperties properties = new NacosDiscoveryProperties();
        properties.setNamespace(namespace);
        properties.setGroup(group);
        return properties;
    }

    private static Route routeWithDiscovery(Map<String, Object> discovery) {
        return Route.async()
                .id("user-service-gray")
                .uri("lb://user-service")
                .predicate(exchange -> true)
                .metadata(Map.of("discovery", discovery))
                .build();
    }
}
```

- [ ] **Step 3: 运行新测试，确认通过**

```bash
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.model.AegisDiscoveryMetadataTest"
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 4: 更新 NamespaceAwareNacosServiceInstanceListSupplier 的 import**

修改 `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplier.java`，将：

```java
import io.aegis.gateway.loadbalancer.discovery.AegisDiscoveryMetadata;
```

改为：

```java
import io.aegis.gateway.core.model.AegisDiscoveryMetadata;
```

（该文件其他内容不变。）

- [ ] **Step 5: 删除 loadbalancer 中的旧文件**

```bash
git rm gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadata.java
git rm gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadataTest.java
```

- [ ] **Step 6: 更新 NamespaceAwareNacosServiceInstanceListSupplierTest 的 import**

修改 `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplierTest.java`，将：

```java
import io.aegis.gateway.loadbalancer.discovery.AegisDiscoveryMetadata;
```

改为：

```java
import io.aegis.gateway.core.model.AegisDiscoveryMetadata;
```

- [ ] **Step 7: 运行 loadbalancer 全量测试**

```bash
./gradlew :gateway-loadbalancer:test
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 8: 提交 Task 1**

```bash
git add gateway-core/src/main/java/io/aegis/gateway/core/model/AegisDiscoveryMetadata.java \
        gateway-core/src/test/java/io/aegis/gateway/core/model/AegisDiscoveryMetadataTest.java \
        gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplier.java \
        gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplierTest.java
git commit -m "refactor(core): move AegisDiscoveryMetadata to gateway-core as shared contract"
```

---

## Task 2: NamespaceAwareNacosServiceInstanceListSupplier 增加 exchange attribute 优先级

**Files:**
- Modify: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplier.java`
- Modify: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplierTest.java`

- [ ] **Step 1: 新增失败测试**

在 `NamespaceAwareNacosServiceInstanceListSupplierTest.java` 末尾，在类的最后一个 `}` 前添加：

```java
@Test
void get_shouldUseExchangeAttributeOverRouteMetadata() throws Exception {
    NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
    NacosNamingServiceRegistry registry = mock(NacosNamingServiceRegistry.class);
    NamingService namingService = mock(NamingService.class);
    when(registry.getNamingService(new AegisDiscoveryMetadata("prod-canary", "DEFAULT_GROUP"))).thenReturn(namingService);
    when(namingService.selectInstances("user-service", "DEFAULT_GROUP", true)).thenReturn(List.of(instance()));
    NamespaceAwareNacosServiceInstanceListSupplier supplier =
            new NamespaceAwareNacosServiceInstanceListSupplier("user-service", defaults, registry);

    // route metadata 指向 "gray"，exchange attribute 指向 "prod-canary"，attribute 应优先
    StepVerifier.create(supplier.get(requestWithGrayAttribute(
                    route("user-service-gray", "gray", "GROUP_A"),
                    new AegisDiscoveryMetadata("prod-canary", "DEFAULT_GROUP"))).next())
            .assertNext(instances -> assertThat(instances).hasSize(1))
            .verifyComplete();

    verify(namingService).selectInstances("user-service", "DEFAULT_GROUP", true);
}

private static Request<RequestDataContext> requestWithGrayAttribute(Route route, AegisDiscoveryMetadata grayMetadata) {
    MockServerWebExchange exchange = MockServerWebExchange.from(
            MockServerHttpRequest.get("/api/users/1").build());
    exchange.getAttributes().put(GATEWAY_ROUTE_ATTR, route);
    exchange.getAttributes().put(AegisDiscoveryMetadata.ATTR_KEY, grayMetadata);
    RequestData requestData = new RequestData(exchange.getRequest(), exchange.getAttributes());
    return new DefaultRequest<>(new RequestDataContext(requestData, "default"));
}
```

- [ ] **Step 2: 运行失败测试，确认失败**

```bash
./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.discovery.NamespaceAwareNacosServiceInstanceListSupplierTest.get_shouldUseExchangeAttributeOverRouteMetadata"
```

Expected: FAIL（当前实现没有读取 exchange attribute）

- [ ] **Step 3: 修改 NamespaceAwareNacosServiceInstanceListSupplier**

在 `NamespaceAwareNacosServiceInstanceListSupplier.java` 中，用以下代码**替换**整个 `loadInstances` 方法，并在类中**新增** `extractGrayMetadata` 方法：

将 `loadInstances` 方法改为：

```java
private List<ServiceInstance> loadInstances(Request request) throws Exception {
    // 优先读取 GrayRoutingFilter 写入的 exchange attribute，再回退到路由 metadata / 全局默认值
    AegisDiscoveryMetadata metadata = extractGrayMetadata(request);
    if (metadata == null) {
        Route route = extractRoute(request);
        metadata = AegisDiscoveryMetadata.from(route, discoveryProperties);
    }
    NamingService namingService = namingServiceRegistry.getNamingService(metadata);
    List<Instance> instances = namingService.selectInstances(serviceId, metadata.group(), true);
    List<ServiceInstance> serviceInstances = instances.stream()
            .map(instance -> toServiceInstance(instance, metadata))
            .flatMap(List::stream)
            .toList();
    if (log.isDebugEnabled()) {
        log.debug("Loaded Nacos instances serviceId={}, namespace={}, group={}, count={}",
                serviceId, metadata.namespace(), metadata.group(), serviceInstances.size());
    }
    return serviceInstances;
}
```

在 `extractRoute` 方法之后新增：

```java
private AegisDiscoveryMetadata extractGrayMetadata(Request request) {
    if (request == null || !(request.getContext() instanceof RequestDataContext context)) {
        return null;
    }
    RequestData requestData = context.getClientRequest();
    if (requestData == null) {
        return null;
    }
    Object attr = requestData.getAttributes().get(AegisDiscoveryMetadata.ATTR_KEY);
    return attr instanceof AegisDiscoveryMetadata m ? m : null;
}
```

- [ ] **Step 4: 运行全量 loadbalancer 测试**

```bash
./gradlew :gateway-loadbalancer:test
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 5: 提交 Task 2**

```bash
git add gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplier.java \
        gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplierTest.java
git commit -m "feat(loadbalancer): prioritize gray exchange attribute over route metadata in instance selection"
```

---

## Task 3: 实现灰度规则模型（gateway-gray）

**Files:**
- Modify: `gateway-gray/build.gradle`
- Create: `gateway-gray/src/main/java/io/aegis/gateway/gray/matcher/GrayMatcher.java`
- Create: `gateway-gray/src/main/java/io/aegis/gateway/gray/matcher/HeaderGrayMatcher.java`
- Create: `gateway-gray/src/main/java/io/aegis/gateway/gray/model/GrayRule.java`
- Create: `gateway-gray/src/main/java/io/aegis/gateway/gray/model/GrayConfig.java`
- Create: `gateway-gray/src/test/java/io/aegis/gateway/gray/matcher/HeaderGrayMatcherTest.java`
- Create: `gateway-gray/src/test/java/io/aegis/gateway/gray/model/GrayRuleTest.java`

- [ ] **Step 1: 更新 gateway-gray/build.gradle**

将 `gateway-gray/build.gradle` 内容替换为：

```gradle
dependencies {
    implementation project(':gateway-core')
    compileOnly 'org.springframework.cloud:spring-cloud-starter-gateway-server-webflux'
    testImplementation 'org.springframework.cloud:spring-cloud-starter-gateway-server-webflux'
}
```

- [ ] **Step 2: 编写 HeaderGrayMatcher 失败测试**

创建 `gateway-gray/src/test/java/io/aegis/gateway/gray/matcher/HeaderGrayMatcherTest.java`：

```java
package io.aegis.gateway.gray.matcher;

import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;

import static org.assertj.core.api.Assertions.assertThat;

class HeaderGrayMatcherTest {

    @Test
    void matches_shouldReturnTrueWhenHeaderValueMatches() {
        HeaderGrayMatcher matcher = new HeaderGrayMatcher("X-User-Type", "beta");
        assertThat(matcher.matches(exchange("X-User-Type", "beta"))).isTrue();
    }

    @Test
    void matches_shouldReturnFalseWhenHeaderValueDiffers() {
        HeaderGrayMatcher matcher = new HeaderGrayMatcher("X-User-Type", "beta");
        assertThat(matcher.matches(exchange("X-User-Type", "stable"))).isFalse();
    }

    @Test
    void matches_shouldReturnFalseWhenHeaderAbsent() {
        HeaderGrayMatcher matcher = new HeaderGrayMatcher("X-User-Type", "beta");
        assertThat(matcher.matches(exchange("X-Other-Header", "beta"))).isFalse();
    }

    @Test
    void matches_shouldBeCaseSensitive() {
        HeaderGrayMatcher matcher = new HeaderGrayMatcher("X-User-Type", "beta");
        assertThat(matcher.matches(exchange("X-User-Type", "Beta"))).isFalse();
    }

    private static ServerWebExchange exchange(String headerName, String headerValue) {
        return MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/test").header(headerName, headerValue).build());
    }
}
```

- [ ] **Step 3: 运行失败测试，确认编译失败**

```bash
./gradlew :gateway-gray:test --tests "io.aegis.gateway.gray.matcher.HeaderGrayMatcherTest"
```

Expected: 编译失败（类不存在）

- [ ] **Step 4: 创建 GrayMatcher 接口**

创建 `gateway-gray/src/main/java/io/aegis/gateway/gray/matcher/GrayMatcher.java`：

```java
package io.aegis.gateway.gray.matcher;

import org.springframework.web.server.ServerWebExchange;

/**
 * 灰度路由匹配策略接口。
 * <p>
 * 新增匹配类型只需实现此接口，并在 {@code GrayRule.toMatcher()} 的 switch 中注册，
 * 不需要修改 filter 主流程。
 */
public interface GrayMatcher {
    boolean matches(ServerWebExchange exchange);
}
```

- [ ] **Step 5: 创建 HeaderGrayMatcher**

创建 `gateway-gray/src/main/java/io/aegis/gateway/gray/matcher/HeaderGrayMatcher.java`：

```java
package io.aegis.gateway.gray.matcher;

import org.springframework.web.server.ServerWebExchange;

/** 精确匹配请求头 key=value 的灰度匹配器。 */
public record HeaderGrayMatcher(String key, String value) implements GrayMatcher {

    @Override
    public boolean matches(ServerWebExchange exchange) {
        return value.equals(exchange.getRequest().getHeaders().getFirst(key));
    }
}
```

- [ ] **Step 6: 运行 HeaderGrayMatcherTest，确认通过**

```bash
./gradlew :gateway-gray:test --tests "io.aegis.gateway.gray.matcher.HeaderGrayMatcherTest"
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 7: 编写 GrayRule 失败测试**

创建 `gateway-gray/src/test/java/io/aegis/gateway/gray/model/GrayRuleTest.java`：

```java
package io.aegis.gateway.gray.model;

import io.aegis.gateway.gray.matcher.HeaderGrayMatcher;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GrayRuleTest {

    @Test
    void toMatcher_shouldReturnHeaderGrayMatcherForHeaderType() {
        GrayRule rule = new GrayRule("header", "X-User-Type", "beta", "user-service-canary");
        assertThat(rule.toMatcher()).isInstanceOf(HeaderGrayMatcher.class);
    }

    @Test
    void toMatcher_shouldThrowIllegalArgumentExceptionForUnknownType() {
        GrayRule rule = new GrayRule("jwt", "userGroup", "beta", "user-service-canary");
        assertThatThrownBy(rule::toMatcher)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown gray rule type: jwt");
    }
}
```

- [ ] **Step 8: 运行失败测试，确认编译失败**

```bash
./gradlew :gateway-gray:test --tests "io.aegis.gateway.gray.model.GrayRuleTest"
```

Expected: 编译失败

- [ ] **Step 9: 创建 GrayRule**

创建 `gateway-gray/src/main/java/io/aegis/gateway/gray/model/GrayRule.java`：

```java
package io.aegis.gateway.gray.model;

import io.aegis.gateway.gray.matcher.GrayMatcher;
import io.aegis.gateway.gray.matcher.HeaderGrayMatcher;

/**
 * 单条灰度路由规则。
 *
 * @param type          匹配器类型，当前支持 {@code "header"}
 * @param key           匹配的请求头名称
 * @param value         期望的请求头值（精确匹配）
 * @param targetRouteId 命中时使用哪条路由的 {@code metadata.discovery} 作为服务发现坐标
 */
public record GrayRule(String type, String key, String value, String targetRouteId) {

    public GrayMatcher toMatcher() {
        return switch (type) {
            case "header" -> new HeaderGrayMatcher(key, value);
            default -> throw new IllegalArgumentException("Unknown gray rule type: " + type);
        };
    }
}
```

- [ ] **Step 10: 创建 GrayConfig**

创建 `gateway-gray/src/main/java/io/aegis/gateway/gray/model/GrayConfig.java`：

```java
package io.aegis.gateway.gray.model;

import java.util.List;

/** 从 {@code aegis-governance.json} 反序列化的灰度路由配置，持有有序规则列表，第一条命中的规则生效。 */
public record GrayConfig(List<GrayRule> rules) {
    public GrayConfig {
        rules = rules == null ? List.of() : List.copyOf(rules);
    }
}
```

- [ ] **Step 11: 运行灰度模型全量测试**

```bash
./gradlew :gateway-gray:test --tests "io.aegis.gateway.gray.matcher.*" --tests "io.aegis.gateway.gray.model.*"
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 12: 提交 Task 3**

```bash
git add gateway-gray/build.gradle \
        gateway-gray/src/main/java/io/aegis/gateway/gray/matcher/GrayMatcher.java \
        gateway-gray/src/main/java/io/aegis/gateway/gray/matcher/HeaderGrayMatcher.java \
        gateway-gray/src/main/java/io/aegis/gateway/gray/model/GrayRule.java \
        gateway-gray/src/main/java/io/aegis/gateway/gray/model/GrayConfig.java \
        gateway-gray/src/test/java/io/aegis/gateway/gray/matcher/HeaderGrayMatcherTest.java \
        gateway-gray/src/test/java/io/aegis/gateway/gray/model/GrayRuleTest.java
git commit -m "feat(gray): add gray routing rule models and header matcher"
```

---

## Task 4: 实现 GrayRoutingFilter

**Files:**
- Create: `gateway-gray/src/main/java/io/aegis/gateway/gray/filter/GrayRoutingFilter.java`
- Create: `gateway-gray/src/test/java/io/aegis/gateway/gray/filter/GrayRoutingFilterTest.java`

- [ ] **Step 1: 编写 GrayRoutingFilter 失败测试**

创建 `gateway-gray/src/test/java/io/aegis/gateway/gray/filter/GrayRoutingFilterTest.java`：

```java
package io.aegis.gateway.gray.filter;

import io.aegis.gateway.core.filter.AegisFilterOrder;
import io.aegis.gateway.core.model.AegisDiscoveryMetadata;
import io.aegis.gateway.core.model.config.AegisRoute;
import io.aegis.gateway.core.model.config.AegisRoutesConfig;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GrayRoutingFilterTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private NacosConfigSyncService syncService;

    @BeforeEach
    void setUp() {
        syncService = mock(NacosConfigSyncService.class);
    }

    @Test
    void filter_shouldSetExchangeAttributeWhenRuleMatches() {
        when(syncService.getGovernanceConfigJson()).thenReturn("""
                {"gray":{"rules":[{"type":"header","key":"X-User-Type","value":"beta","targetRouteId":"user-canary"}]}}
                """);
        when(syncService.getRoutesConfig()).thenReturn(routesConfig("user-canary", "prod-canary", "DEFAULT_GROUP"));
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);
        ServerWebExchange exchange = exchange("X-User-Type", "beta");
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.<AegisDiscoveryMetadata>getAttribute(AegisDiscoveryMetadata.ATTR_KEY))
                .isEqualTo(new AegisDiscoveryMetadata("prod-canary", "DEFAULT_GROUP"));
        verify(chain).filter(exchange);
    }

    @Test
    void filter_shouldPassThroughWhenHeaderValueDoesNotMatch() {
        when(syncService.getGovernanceConfigJson()).thenReturn("""
                {"gray":{"rules":[{"type":"header","key":"X-User-Type","value":"beta","targetRouteId":"user-canary"}]}}
                """);
        when(syncService.getRoutesConfig()).thenReturn(routesConfig("user-canary", "prod-canary", "DEFAULT_GROUP"));
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);
        ServerWebExchange exchange = exchange("X-User-Type", "stable");
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.getAttributes()).doesNotContainKey(AegisDiscoveryMetadata.ATTR_KEY);
        verify(chain).filter(exchange);
    }

    @Test
    void filter_shouldPassThroughWhenNoRulesConfigured() {
        when(syncService.getGovernanceConfigJson()).thenReturn("{}");
        when(syncService.getRoutesConfig()).thenReturn(AegisRoutesConfig.empty());
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);
        ServerWebExchange exchange = exchange("X-User-Type", "beta");
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.getAttributes()).doesNotContainKey(AegisDiscoveryMetadata.ATTR_KEY);
    }

    @Test
    void filter_shouldPassThroughWhenTargetRouteIdNotFoundInRouteMap() {
        when(syncService.getGovernanceConfigJson()).thenReturn("""
                {"gray":{"rules":[{"type":"header","key":"X-User-Type","value":"beta","targetRouteId":"nonexistent"}]}}
                """);
        when(syncService.getRoutesConfig()).thenReturn(AegisRoutesConfig.empty());
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);
        ServerWebExchange exchange = exchange("X-User-Type", "beta");
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.getAttributes()).doesNotContainKey(AegisDiscoveryMetadata.ATTR_KEY);
        verify(chain).filter(exchange);
    }

    @Test
    void filter_shouldUseCustomGovernanceKey() {
        when(syncService.getGovernanceConfigJson()).thenReturn("""
                {"canary":{"rules":[{"type":"header","key":"X-User-Type","value":"beta","targetRouteId":"user-canary"}]}}
                """);
        when(syncService.getRoutesConfig()).thenReturn(routesConfig("user-canary", "prod-canary", "DEFAULT_GROUP"));
        GrayRoutingFilter filter = new GrayRoutingFilter("canary", objectMapper, syncService);
        ServerWebExchange exchange = exchange("X-User-Type", "beta");
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.<AegisDiscoveryMetadata>getAttribute(AegisDiscoveryMetadata.ATTR_KEY))
                .isEqualTo(new AegisDiscoveryMetadata("prod-canary", "DEFAULT_GROUP"));
    }

    @Test
    void getOrder_shouldReturnGrayFilterOrder() {
        when(syncService.getGovernanceConfigJson()).thenReturn("{}");
        when(syncService.getRoutesConfig()).thenReturn(AegisRoutesConfig.empty());
        GrayRoutingFilter filter = new GrayRoutingFilter("gray", objectMapper, syncService);

        assertThat(filter.getOrder()).isEqualTo(AegisFilterOrder.GRAY);
    }

    private static AegisRoutesConfig routesConfig(String routeId, String namespace, String group) {
        AegisRoute route = new AegisRoute(
                routeId, "lb://user-service", List.of("Path=/api/**"), List.of(), 0,
                Map.of("discovery", Map.of("namespace", namespace, "group", group)));
        return new AegisRoutesConfig(List.of(route));
    }

    private static ServerWebExchange exchange(String headerName, String headerValue) {
        return MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/test").header(headerName, headerValue).build());
    }
}
```

- [ ] **Step 2: 运行失败测试，确认编译失败**

```bash
./gradlew :gateway-gray:test --tests "io.aegis.gateway.gray.filter.GrayRoutingFilterTest"
```

Expected: 编译失败

- [ ] **Step 3: 创建 GrayRoutingFilter**

创建 `gateway-gray/src/main/java/io/aegis/gateway/gray/filter/GrayRoutingFilter.java`：

```java
package io.aegis.gateway.gray.filter;

import io.aegis.gateway.core.filter.AegisFilterOrder;
import io.aegis.gateway.core.model.AegisDiscoveryMetadata;
import io.aegis.gateway.core.model.config.AegisRoute;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.gray.model.GrayConfig;
import io.aegis.gateway.gray.model.GrayRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 基于请求头的动态灰度路由 filter。
 * <p>
 * 从 {@code aegis-governance.json} 的 {@code governanceKey} 节点加载 {@link GrayConfig}；
 * 从 {@code aegis-routes.json} 构建 routeId → {@link AegisDiscoveryMetadata} 映射。
 * 每个请求逐条匹配规则，命中后将目标路由的服务发现坐标写入 exchange attribute，
 * 由 {@code gateway-loadbalancer} 在实例选择时优先读取。
 */
public class GrayRoutingFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(GrayRoutingFilter.class);

    private final String governanceKey;
    private final ObjectMapper objectMapper;
    private final AtomicReference<GrayConfig> grayConfig = new AtomicReference<>(new GrayConfig(List.of()));
    private final AtomicReference<Map<String, AegisDiscoveryMetadata>> routeDiscoveryMap =
            new AtomicReference<>(Map.of());

    public GrayRoutingFilter(String governanceKey, ObjectMapper objectMapper,
                              NacosConfigSyncService syncService) {
        this.governanceKey = governanceKey;
        this.objectMapper = objectMapper;
        syncService.registerGovernanceListener(this::onGovernanceUpdate);
        onGovernanceUpdate(syncService.getGovernanceConfigJson());
        syncService.registerRoutesListener(config -> onRoutesUpdate(config.routes()));
        onRoutesUpdate(syncService.getRoutesConfig().routes());
    }

    private void onGovernanceUpdate(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            JsonNode node = root.path(governanceKey);
            if (node.isMissingNode() || node.isNull()) {
                grayConfig.set(new GrayConfig(List.of()));
                return;
            }
            grayConfig.set(objectMapper.treeToValue(node, GrayConfig.class));
        } catch (Exception e) {
            log.error("Failed to parse gray config under governance key '{}'", governanceKey, e);
        }
    }

    private void onRoutesUpdate(List<AegisRoute> routes) {
        Map<String, AegisDiscoveryMetadata> map = new HashMap<>();
        for (AegisRoute route : routes) {
            AegisDiscoveryMetadata metadata = parseDiscoveryMetadata(route.metadata());
            if (metadata != null) {
                map.put(route.id(), metadata);
            }
        }
        routeDiscoveryMap.set(Map.copyOf(map));
    }

    private AegisDiscoveryMetadata parseDiscoveryMetadata(Map<String, Object> routeMetadata) {
        Object discovery = routeMetadata.get(AegisDiscoveryMetadata.DISCOVERY_METADATA_KEY);
        if (!(discovery instanceof Map<?, ?> discoveryMap)) {
            return null;
        }
        Object namespace = discoveryMap.get(AegisDiscoveryMetadata.NAMESPACE_KEY);
        Object group = discoveryMap.get(AegisDiscoveryMetadata.GROUP_KEY);
        if (!(namespace instanceof String ns) || !(group instanceof String g)) {
            return null;
        }
        return new AegisDiscoveryMetadata(ns, g);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        GrayConfig config = grayConfig.get();
        for (GrayRule rule : config.rules()) {
            if (rule.toMatcher().matches(exchange)) {
                AegisDiscoveryMetadata metadata = routeDiscoveryMap.get().get(rule.targetRouteId());
                if (metadata != null) {
                    exchange.getAttributes().put(AegisDiscoveryMetadata.ATTR_KEY, metadata);
                    log.debug("Gray routing matched: targetRouteId={}, namespace={}, group={}",
                            rule.targetRouteId(), metadata.namespace(), metadata.group());
                } else {
                    log.warn("Gray routing: targetRouteId={} has no discovery metadata, pass through",
                            rule.targetRouteId());
                }
                return chain.filter(exchange);
            }
        }
        return chain.filter(exchange);
    }

    @Override
    public int getOrder() {
        return AegisFilterOrder.GRAY;
    }
}
```

- [ ] **Step 4: 运行 GrayRoutingFilterTest，确认通过**

```bash
./gradlew :gateway-gray:test --tests "io.aegis.gateway.gray.filter.GrayRoutingFilterTest"
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 5: 提交 Task 4**

```bash
git add gateway-gray/src/main/java/io/aegis/gateway/gray/filter/GrayRoutingFilter.java \
        gateway-gray/src/test/java/io/aegis/gateway/gray/filter/GrayRoutingFilterTest.java
git commit -m "feat(gray): implement GrayRoutingFilter with header-based rule matching"
```

---

## Task 5: 自动配置与最终验证

**Files:**
- Create: `gateway-gray/src/main/java/io/aegis/gateway/gray/config/GrayAutoConfiguration.java`
- Create: `gateway-gray/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Create: `gateway-gray/src/test/java/io/aegis/gateway/gray/config/GrayAutoConfigurationTest.java`

- [ ] **Step 1: 编写自动配置测试**

创建 `gateway-gray/src/test/java/io/aegis/gateway/gray/config/GrayAutoConfigurationTest.java`：

```java
package io.aegis.gateway.gray.config;

import io.aegis.gateway.core.model.config.AegisRoutesConfig;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.gray.filter.GrayRoutingFilter;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GrayAutoConfigurationTest {

    @Test
    void autoConfiguration_shouldRegisterGrayRoutingFilter() {
        NacosConfigSyncService mockSyncService = mock(NacosConfigSyncService.class);
        when(mockSyncService.getGovernanceConfigJson()).thenReturn("{}");
        when(mockSyncService.getRoutesConfig()).thenReturn(AegisRoutesConfig.empty());

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(GrayAutoConfiguration.class))
                .withBean(NacosConfigSyncService.class, () -> mockSyncService)
                .withBean(ObjectMapper.class, tools.jackson.databind.ObjectMapper::new)
                .run(context -> assertThat(context).hasSingleBean(GrayRoutingFilter.class));
    }

    @Test
    void autoConfiguration_shouldRespectCustomGovernanceKey() {
        NacosConfigSyncService mockSyncService = mock(NacosConfigSyncService.class);
        when(mockSyncService.getGovernanceConfigJson()).thenReturn("{}");
        when(mockSyncService.getRoutesConfig()).thenReturn(AegisRoutesConfig.empty());

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(GrayAutoConfiguration.class))
                .withBean(NacosConfigSyncService.class, () -> mockSyncService)
                .withBean(ObjectMapper.class, tools.jackson.databind.ObjectMapper::new)
                .withPropertyValues("spring.aegis.gray.governance-key=canary")
                .run(context -> assertThat(context).hasSingleBean(GrayRoutingFilter.class));
    }
}
```

- [ ] **Step 2: 运行失败测试，确认编译失败**

```bash
./gradlew :gateway-gray:test --tests "io.aegis.gateway.gray.config.GrayAutoConfigurationTest"
```

Expected: 编译失败

- [ ] **Step 3: 创建 GrayAutoConfiguration**

创建 `gateway-gray/src/main/java/io/aegis/gateway/gray/config/GrayAutoConfiguration.java`：

```java
package io.aegis.gateway.gray.config;

import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.gray.filter.GrayRoutingFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.ObjectMapper;

/**
 * gateway-gray 模块的自动配置入口，注册 {@link GrayRoutingFilter} Bean。
 * <p>
 * 通过 {@code spring.aegis.gray.governance-key}（默认 {@code "gray"}）指定
 * {@code aegis-governance.json} 中灰度规则节点的 key，允许用户使用语义化名称（如 canary、staging）。
 */
@AutoConfiguration
@ConditionalOnClass(GlobalFilter.class)
@ConditionalOnBean(NacosConfigSyncService.class)
public class GrayAutoConfiguration {

    @Value("${spring.aegis.gray.governance-key:gray}")
    private String governanceKey;

    @Bean
    @ConditionalOnMissingBean
    public GrayRoutingFilter grayRoutingFilter(NacosConfigSyncService syncService,
                                               ObjectMapper objectMapper) {
        return new GrayRoutingFilter(governanceKey, objectMapper, syncService);
    }
}
```

- [ ] **Step 4: 创建 AutoConfiguration.imports**

创建 `gateway-gray/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`：

```
io.aegis.gateway.gray.config.GrayAutoConfiguration
```

- [ ] **Step 5: 运行 gateway-gray 全量测试**

```bash
./gradlew :gateway-gray:test
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 6: 运行全项目测试**

```bash
./gradlew test
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 7: 提交 Task 5**

```bash
git add gateway-gray/src/main/java/io/aegis/gateway/gray/config/GrayAutoConfiguration.java \
        gateway-gray/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports \
        gateway-gray/src/test/java/io/aegis/gateway/gray/config/GrayAutoConfigurationTest.java
git commit -m "feat(gray): register GrayRoutingFilter via auto-configuration"
```

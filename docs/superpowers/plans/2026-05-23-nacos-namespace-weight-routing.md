# Nacos Namespace Weight Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build namespace-level weighted routing for the same Nacos service name by using SCG `Weight` routes plus a namespace-aware `ServiceInstanceListSupplier`.

**Architecture:** SCG keeps matching virtual routes with `Weight` and `lb://service-name`. After a route is selected, `gateway-loadbalancer` reads `metadata.discovery.namespace/group` from the selected `Route` in LoadBalancer request attributes, queries that Nacos namespace only, and returns healthy instances to the existing Spring Cloud LoadBalancer / Nacos LoadBalancer selection flow.

**Tech Stack:** Java 25, Spring Boot 4.0.6, Spring Cloud Gateway 5.0.1, Spring Cloud LoadBalancer 5.0.1, Spring Cloud Alibaba Nacos 2025.1.0.0, Nacos Client 3.1.1, JUnit 5, Mockito, Reactor Test.

---

## File Structure

- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadata.java`
  - Parses `Route.getMetadata().get("discovery")` and falls back to default Nacos discovery properties.
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NacosNamingServiceRegistry.java`
  - Caches one `NamingService` per namespace and shuts them down on application stop.
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplier.java`
  - Implements request-aware service instance lookup from selected route metadata.
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisNamespaceLoadBalancerClientConfiguration.java`
  - Provides the per-service child-context `ServiceInstanceListSupplier`.
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfiguration.java`
  - Registers the registry and attaches the child-context LoadBalancer configuration.
- Create: `gateway-loadbalancer/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
  - Exposes the auto configuration.
- Create: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadataTest.java`
- Create: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NacosNamingServiceRegistryTest.java`
- Create: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplierTest.java`
- Create: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfigurationTest.java`
- Modify: `README.md`
  - Add a route example for namespace-level weighted routing.

## Pre-flight

- [ ] **Step 1: Confirm unrelated local changes are not part of this implementation**

Run:

```bash
git status --short --branch
```

Expected: if `gateway-core/src/main/java/io/aegis/gateway/core/filter/AegisFilterOrder.java` is still modified, leave it untouched unless the user explicitly includes it.

- [ ] **Step 2: Confirm the plan target module compiles before changes**

Run:

```bash
./gradlew :gateway-loadbalancer:test
```

Expected: `BUILD SUCCESSFUL`.

---

### Task 1: Parse Route Discovery Metadata

**Files:**
- Create: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadataTest.java`
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadata.java`

- [ ] **Step 1: Write the failing metadata parser tests**

Create `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadataTest.java`:

```java
package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.route.Route;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AegisDiscoveryMetadataTest {

    @Test
    void from_shouldReadNamespaceAndGroupFromRouteMetadata() {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        Route route = routeWithDiscovery(Map.of(
                "namespace", "gray",
                "group", "GROUP_A"));

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
        Route route = routeWithDiscovery(Map.of(
                "namespace", 123,
                "group", true));

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

- [ ] **Step 2: Run the metadata parser tests and verify they fail**

Run:

```bash
./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.discovery.AegisDiscoveryMetadataTest"
```

Expected: compile failure because `AegisDiscoveryMetadata` does not exist.

- [ ] **Step 3: Add the metadata parser**

Create `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadata.java`:

```java
package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Objects;

public record AegisDiscoveryMetadata(String namespace, String group) {

    private static final Logger log = LoggerFactory.getLogger(AegisDiscoveryMetadata.class);

    public static final String DISCOVERY_METADATA_KEY = "discovery";
    public static final String NAMESPACE_KEY = "namespace";
    public static final String GROUP_KEY = "group";
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

- [ ] **Step 4: Run the metadata parser tests and verify they pass**

Run:

```bash
./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.discovery.AegisDiscoveryMetadataTest"
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit Task 1**

Run:

```bash
git add gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadataTest.java \
        gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/AegisDiscoveryMetadata.java
git commit -m "feat(loadbalancer): parse Nacos discovery route metadata"
```

Expected: commit succeeds and includes only the two metadata parser files.

---

### Task 2: Cache Nacos NamingService By Namespace

**Files:**
- Create: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NacosNamingServiceRegistryTest.java`
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NacosNamingServiceRegistry.java`

- [ ] **Step 1: Write the failing registry tests**

Create `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NacosNamingServiceRegistryTest.java`:

```java
package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.naming.NamingService;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NacosNamingServiceRegistryTest {

    @Test
    void getNamingService_shouldCreateServiceWithRequestedNamespace() {
        NacosDiscoveryProperties defaults = discoveryProperties();
        List<Properties> createdProperties = new ArrayList<>();
        NamingService namingService = mock(NamingService.class);
        NacosNamingServiceRegistry registry = new NacosNamingServiceRegistry(defaults, properties -> {
            createdProperties.add(properties);
            return namingService;
        });

        NamingService result = registry.getNamingService(new AegisDiscoveryMetadata("gray", "GROUP_A"));

        assertThat(result).isSameAs(namingService);
        assertThat(createdProperties).hasSize(1);
        assertThat(createdProperties.getFirst().getProperty(PropertyKeyConst.SERVER_ADDR)).isEqualTo("127.0.0.1:8848");
        assertThat(createdProperties.getFirst().getProperty(PropertyKeyConst.NAMESPACE)).isEqualTo("gray");
    }

    @Test
    void getNamingService_shouldReuseServiceForSameNamespace() {
        NacosDiscoveryProperties defaults = discoveryProperties();
        NamingService namingService = mock(NamingService.class);
        NacosNamingServiceRegistry registry = new NacosNamingServiceRegistry(defaults, properties -> namingService);

        NamingService first = registry.getNamingService(new AegisDiscoveryMetadata("gray", "GROUP_A"));
        NamingService second = registry.getNamingService(new AegisDiscoveryMetadata("gray", "GROUP_B"));

        assertThat(first).isSameAs(second);
    }

    @Test
    void shutdown_shouldCloseCreatedServices() throws Exception {
        NacosDiscoveryProperties defaults = discoveryProperties();
        NamingService grayService = mock(NamingService.class);
        NamingService devService = mock(NamingService.class);
        NacosNamingServiceRegistry registry = new NacosNamingServiceRegistry(defaults, properties -> {
            String namespace = properties.getProperty(PropertyKeyConst.NAMESPACE);
            return "gray".equals(namespace) ? grayService : devService;
        });

        registry.getNamingService(new AegisDiscoveryMetadata("gray", "DEFAULT_GROUP"));
        registry.getNamingService(new AegisDiscoveryMetadata("dev", "DEFAULT_GROUP"));
        registry.shutdown();

        verify(grayService).shutDown();
        verify(devService).shutDown();
    }

    private static NacosDiscoveryProperties discoveryProperties() {
        NacosDiscoveryProperties properties = mock(NacosDiscoveryProperties.class);
        Properties nacosProperties = new Properties();
        nacosProperties.put(PropertyKeyConst.SERVER_ADDR, "127.0.0.1:8848");
        nacosProperties.put(PropertyKeyConst.NAMESPACE, "public");
        when(properties.getNacosProperties()).thenReturn(nacosProperties);
        return properties;
    }
}
```

- [ ] **Step 2: Run the registry tests and verify they fail**

Run:

```bash
./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.discovery.NacosNamingServiceRegistryTest"
```

Expected: compile failure because `NacosNamingServiceRegistry` does not exist.

- [ ] **Step 3: Add the registry**

Create `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NacosNamingServiceRegistry.java`:

```java
package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NacosNamingServiceRegistry {

    private static final Logger log = LoggerFactory.getLogger(NacosNamingServiceRegistry.class);

    private final NacosDiscoveryProperties discoveryProperties;
    private final NamingServiceFactory factory;
    private final ConcurrentMap<String, NamingService> namingServices = new ConcurrentHashMap<>();

    public NacosNamingServiceRegistry(NacosDiscoveryProperties discoveryProperties) {
        this(discoveryProperties, NamingFactory::createNamingService);
    }

    NacosNamingServiceRegistry(NacosDiscoveryProperties discoveryProperties, NamingServiceFactory factory) {
        this.discoveryProperties = discoveryProperties;
        this.factory = factory;
    }

    public NamingService getNamingService(AegisDiscoveryMetadata metadata) {
        return namingServices.computeIfAbsent(metadata.namespace(), this::createNamingService);
    }

    private NamingService createNamingService(String namespace) {
        Properties properties = new Properties();
        properties.putAll(discoveryProperties.getNacosProperties());
        properties.put(PropertyKeyConst.NAMESPACE, namespace);
        try {
            return factory.create(properties);
        } catch (NacosException e) {
            throw new IllegalStateException("Failed to create Nacos NamingService for namespace " + namespace, e);
        }
    }

    @PreDestroy
    public void shutdown() {
        namingServices.forEach((namespace, namingService) -> {
            try {
                namingService.shutDown();
            } catch (NacosException e) {
                log.warn("Failed to shut down Nacos NamingService for namespace {}", namespace, e);
            }
        });
        namingServices.clear();
    }

    @FunctionalInterface
    interface NamingServiceFactory {
        NamingService create(Properties properties) throws NacosException;
    }
}
```

- [ ] **Step 4: Run metadata and registry tests**

Run:

```bash
./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.discovery.*Test"
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit Task 2**

Run:

```bash
git add gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NacosNamingServiceRegistryTest.java \
        gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NacosNamingServiceRegistry.java
git commit -m "feat(loadbalancer): cache Nacos naming services by namespace"
```

Expected: commit succeeds and includes only the registry files.

---

### Task 3: Supply Namespace-Aware Service Instances

**Files:**
- Create: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplierTest.java`
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplier.java`

- [ ] **Step 1: Write the failing supplier tests**

Create `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplierTest.java`:

```java
package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.DefaultRequest;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestData;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR;

class NamespaceAwareNacosServiceInstanceListSupplierTest {

    @Test
    void get_shouldQueryNamespaceFromSelectedRouteMetadata() throws Exception {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        NacosNamingServiceRegistry registry = mock(NacosNamingServiceRegistry.class);
        NamingService namingService = mock(NamingService.class);
        when(registry.getNamingService(new AegisDiscoveryMetadata("gray", "GROUP_A"))).thenReturn(namingService);
        when(namingService.selectInstances("user-service", "GROUP_A", true)).thenReturn(List.of(instance()));
        NamespaceAwareNacosServiceInstanceListSupplier supplier =
                new NamespaceAwareNacosServiceInstanceListSupplier("user-service", defaults, registry);

        StepVerifier.create(supplier.get(request(route("user-service-gray", "gray", "GROUP_A"))).next())
                .assertNext(instances -> {
                    assertThat(instances).hasSize(1);
                    ServiceInstance serviceInstance = instances.getFirst();
                    assertThat(serviceInstance.getServiceId()).isEqualTo("user-service");
                    assertThat(serviceInstance.getHost()).isEqualTo("10.0.0.7");
                    assertThat(serviceInstance.getPort()).isEqualTo(8080);
                    assertThat(serviceInstance.getMetadata())
                            .containsEntry("aegis.nacos.namespace", "gray")
                            .containsEntry("aegis.nacos.group", "GROUP_A")
                            .containsEntry("version", "v2");
                })
                .verifyComplete();

        verify(namingService).selectInstances("user-service", "GROUP_A", true);
    }

    @Test
    void get_shouldFallbackToDefaultDiscoveryWhenRouteMetadataIsMissing() throws Exception {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        NacosNamingServiceRegistry registry = mock(NacosNamingServiceRegistry.class);
        NamingService namingService = mock(NamingService.class);
        when(registry.getNamingService(new AegisDiscoveryMetadata("public", "DEFAULT_GROUP"))).thenReturn(namingService);
        when(namingService.selectInstances("user-service", "DEFAULT_GROUP", true)).thenReturn(List.of(instance()));
        NamespaceAwareNacosServiceInstanceListSupplier supplier =
                new NamespaceAwareNacosServiceInstanceListSupplier("user-service", defaults, registry);

        StepVerifier.create(supplier.get(request(routeWithoutMetadata())).next())
                .assertNext(instances -> assertThat(instances).hasSize(1))
                .verifyComplete();

        verify(namingService).selectInstances("user-service", "DEFAULT_GROUP", true);
    }

    @Test
    void get_shouldReturnEmptyListWhenNacosQueryFails() throws Exception {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        NacosNamingServiceRegistry registry = mock(NacosNamingServiceRegistry.class);
        NamingService namingService = mock(NamingService.class);
        when(registry.getNamingService(new AegisDiscoveryMetadata("gray", "GROUP_A"))).thenReturn(namingService);
        when(namingService.selectInstances("user-service", "GROUP_A", true)).thenThrow(new RuntimeException("nacos down"));
        NamespaceAwareNacosServiceInstanceListSupplier supplier =
                new NamespaceAwareNacosServiceInstanceListSupplier("user-service", defaults, registry);

        StepVerifier.create(supplier.get(request(route("user-service-gray", "gray", "GROUP_A"))).next())
                .assertNext(instances -> assertThat(instances).isEmpty())
                .verifyComplete();
    }

    private static NacosDiscoveryProperties defaults(String namespace, String group) {
        NacosDiscoveryProperties properties = new NacosDiscoveryProperties();
        properties.setNamespace(namespace);
        properties.setGroup(group);
        return properties;
    }

    private static Request<RequestDataContext> request(Route route) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/users/1").build());
        exchange.getAttributes().put(GATEWAY_ROUTE_ATTR, route);
        RequestData requestData = new RequestData(exchange.getRequest(), exchange.getAttributes());
        return new DefaultRequest<>(new RequestDataContext(requestData, "default"));
    }

    private static Route route(String routeId, String namespace, String group) {
        return Route.async()
                .id(routeId)
                .uri("lb://user-service")
                .predicate(exchange -> true)
                .metadata(Map.of("discovery", Map.of(
                        "namespace", namespace,
                        "group", group)))
                .build();
    }

    private static Route routeWithoutMetadata() {
        return Route.async()
                .id("user-service-default")
                .uri("lb://user-service")
                .predicate(exchange -> true)
                .build();
    }

    private static Instance instance() {
        Instance instance = new Instance();
        instance.setInstanceId("instance-1");
        instance.setIp("10.0.0.7");
        instance.setPort(8080);
        instance.setHealthy(true);
        instance.setEnabled(true);
        instance.setWeight(10.0);
        instance.setClusterName("HZ");
        instance.setMetadata(Map.of("version", "v2"));
        return instance;
    }
}
```

- [ ] **Step 2: Run the supplier tests and verify they fail**

Run:

```bash
./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.discovery.NamespaceAwareNacosServiceInstanceListSupplierTest"
```

Expected: compile failure because `NamespaceAwareNacosServiceInstanceListSupplier` does not exist.

- [ ] **Step 3: Add the supplier**

Create `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplier.java`:

```java
package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.cloud.nacos.NacosServiceInstance;
import com.alibaba.cloud.nacos.discovery.NacosServiceDiscovery;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestData;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR;

public class NamespaceAwareNacosServiceInstanceListSupplier implements ServiceInstanceListSupplier {

    public static final String AEGIS_NACOS_NAMESPACE = "aegis.nacos.namespace";
    public static final String AEGIS_NACOS_GROUP = "aegis.nacos.group";

    private static final Logger log = LoggerFactory.getLogger(NamespaceAwareNacosServiceInstanceListSupplier.class);

    private final String serviceId;
    private final NacosDiscoveryProperties discoveryProperties;
    private final NacosNamingServiceRegistry namingServiceRegistry;

    public NamespaceAwareNacosServiceInstanceListSupplier(String serviceId,
                                                          NacosDiscoveryProperties discoveryProperties,
                                                          NacosNamingServiceRegistry namingServiceRegistry) {
        this.serviceId = serviceId;
        this.discoveryProperties = discoveryProperties;
        this.namingServiceRegistry = namingServiceRegistry;
    }

    @Override
    public String getServiceId() {
        return serviceId;
    }

    @Override
    public Flux<List<ServiceInstance>> get() {
        return queryInstances(null);
    }

    @Override
    public Flux<List<ServiceInstance>> get(Request request) {
        return queryInstances(request);
    }

    private Flux<List<ServiceInstance>> queryInstances(Request request) {
        return Mono.fromCallable(() -> loadInstances(request))
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(error -> {
                    log.warn("Failed to query Nacos instances for serviceId={}", serviceId, error);
                    return Mono.just(List.of());
                })
                .flux();
    }

    private List<ServiceInstance> loadInstances(Request request) throws Exception {
        Route route = extractRoute(request);
        AegisDiscoveryMetadata metadata = AegisDiscoveryMetadata.from(route, discoveryProperties);
        NamingService namingService = namingServiceRegistry.getNamingService(metadata);
        List<Instance> instances = namingService.selectInstances(serviceId, metadata.group(), true);
        List<ServiceInstance> serviceInstances = instances.stream()
                .map(instance -> toServiceInstance(instance, metadata))
                .flatMap(List::stream)
                .toList();
        if (log.isDebugEnabled()) {
            log.debug("Loaded Nacos instances routeId={}, serviceId={}, namespace={}, group={}, count={}",
                    route == null ? "" : route.getId(),
                    serviceId,
                    metadata.namespace(),
                    metadata.group(),
                    serviceInstances.size());
        }
        return serviceInstances;
    }

    private Route extractRoute(Request request) {
        if (request == null || !(request.getContext() instanceof RequestDataContext context)) {
            return null;
        }
        RequestData requestData = context.getClientRequest();
        if (requestData == null) {
            return null;
        }
        Object route = requestData.getAttributes().get(GATEWAY_ROUTE_ATTR);
        return route instanceof Route selectedRoute ? selectedRoute : null;
    }

    private List<ServiceInstance> toServiceInstance(Instance instance, AegisDiscoveryMetadata metadata) {
        ServiceInstance serviceInstance = NacosServiceDiscovery.hostToServiceInstance(instance, serviceId);
        if (!(serviceInstance instanceof NacosServiceInstance nacosServiceInstance)) {
            return List.of();
        }
        Map<String, String> enrichedMetadata = new HashMap<>(nacosServiceInstance.getMetadata());
        enrichedMetadata.put(AEGIS_NACOS_NAMESPACE, metadata.namespace());
        enrichedMetadata.put(AEGIS_NACOS_GROUP, metadata.group());
        nacosServiceInstance.setMetadata(enrichedMetadata);
        return List.of(nacosServiceInstance);
    }
}
```

- [ ] **Step 4: Run all discovery tests**

Run:

```bash
./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.discovery.*Test"
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit Task 3**

Run:

```bash
git add gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplierTest.java \
        gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/discovery/NamespaceAwareNacosServiceInstanceListSupplier.java
git commit -m "feat(loadbalancer): supply Nacos instances from selected namespace"
```

Expected: commit succeeds and includes only supplier files.

---

### Task 4: Register LoadBalancer Auto Configuration

**Files:**
- Create: `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfigurationTest.java`
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisNamespaceLoadBalancerClientConfiguration.java`
- Create: `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfiguration.java`
- Create: `gateway-loadbalancer/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`

- [ ] **Step 1: Write the failing auto configuration tests**

Create `gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfigurationTest.java`:

```java
package io.aegis.gateway.loadbalancer.config;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import io.aegis.gateway.loadbalancer.discovery.NamespaceAwareNacosServiceInstanceListSupplier;
import io.aegis.gateway.loadbalancer.discovery.NacosNamingServiceRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.cloud.loadbalancer.support.LoadBalancerClientFactory;
import org.springframework.core.env.MapPropertySource;
import org.springframework.mock.env.MockEnvironment;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class AegisLoadBalancerAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(AegisLoadBalancerAutoConfiguration.class))
            .withBean(NacosDiscoveryProperties.class, NacosDiscoveryProperties::new);

    @Test
    void autoConfiguration_shouldRegisterNamingServiceRegistry() {
        contextRunner.run(context ->
                assertThat(context).hasSingleBean(NacosNamingServiceRegistry.class));
    }

    @Test
    void clientConfiguration_shouldCreateNamespaceAwareSupplierForServiceContext() {
        MockEnvironment environment = new MockEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("loadbalancer-client", Map.of(
                LoadBalancerClientFactory.PROPERTY_NAME, "user-service")));
        NacosDiscoveryProperties discoveryProperties = new NacosDiscoveryProperties();
        NacosNamingServiceRegistry registry = mock(NacosNamingServiceRegistry.class);
        AegisNamespaceLoadBalancerClientConfiguration configuration =
                new AegisNamespaceLoadBalancerClientConfiguration();

        ServiceInstanceListSupplier supplier = configuration.aegisServiceInstanceListSupplier(
                environment, discoveryProperties, registry);

        assertThat(supplier).isInstanceOf(NamespaceAwareNacosServiceInstanceListSupplier.class);
        assertThat(supplier.getServiceId()).isEqualTo("user-service");
    }
}
```

- [ ] **Step 2: Run the auto configuration tests and verify they fail**

Run:

```bash
./gradlew :gateway-loadbalancer:test --tests "io.aegis.gateway.loadbalancer.config.AegisLoadBalancerAutoConfigurationTest"
```

Expected: compile failure because the configuration classes do not exist.

- [ ] **Step 3: Add the child LoadBalancer client configuration**

Create `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisNamespaceLoadBalancerClientConfiguration.java`:

```java
package io.aegis.gateway.loadbalancer.config;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import io.aegis.gateway.loadbalancer.discovery.NamespaceAwareNacosServiceInstanceListSupplier;
import io.aegis.gateway.loadbalancer.discovery.NacosNamingServiceRegistry;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.cloud.loadbalancer.support.LoadBalancerClientFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;

@Configuration(proxyBeanMethods = false)
public class AegisNamespaceLoadBalancerClientConfiguration {

    @Bean
    @Primary
    public ServiceInstanceListSupplier aegisServiceInstanceListSupplier(Environment environment,
                                                                       NacosDiscoveryProperties discoveryProperties,
                                                                       NacosNamingServiceRegistry namingServiceRegistry) {
        String serviceId = environment.getProperty(LoadBalancerClientFactory.PROPERTY_NAME);
        Assert.hasText(serviceId, "'serviceId' must not be empty");
        return new NamespaceAwareNacosServiceInstanceListSupplier(serviceId, discoveryProperties, namingServiceRegistry);
    }
}
```

- [ ] **Step 4: Add the top-level auto configuration**

Create `gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfiguration.java`:

```java
package io.aegis.gateway.loadbalancer.config;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.cloud.nacos.loadbalancer.LoadBalancerNacosAutoConfiguration;
import com.alibaba.nacos.api.naming.NamingService;
import io.aegis.gateway.loadbalancer.discovery.NacosNamingServiceRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.loadbalancer.annotation.LoadBalancerClients;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = LoadBalancerNacosAutoConfiguration.class)
@ConditionalOnClass({ NamingService.class, ServiceInstanceListSupplier.class })
@ConditionalOnBean(NacosDiscoveryProperties.class)
@LoadBalancerClients(defaultConfiguration = AegisNamespaceLoadBalancerClientConfiguration.class)
public class AegisLoadBalancerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public NacosNamingServiceRegistry nacosNamingServiceRegistry(NacosDiscoveryProperties discoveryProperties) {
        return new NacosNamingServiceRegistry(discoveryProperties);
    }
}
```

- [ ] **Step 5: Add Boot auto configuration imports**

Create `gateway-loadbalancer/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`:

```text
io.aegis.gateway.loadbalancer.config.AegisLoadBalancerAutoConfiguration
```

- [ ] **Step 6: Run all gateway-loadbalancer tests**

Run:

```bash
./gradlew :gateway-loadbalancer:test
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 7: Commit Task 4**

Run:

```bash
git add gateway-loadbalancer/src/test/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfigurationTest.java \
        gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisNamespaceLoadBalancerClientConfiguration.java \
        gateway-loadbalancer/src/main/java/io/aegis/gateway/loadbalancer/config/AegisLoadBalancerAutoConfiguration.java \
        gateway-loadbalancer/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
git commit -m "feat(loadbalancer): register namespace-aware loadbalancer supplier"
```

Expected: commit succeeds and includes only auto configuration files.

---

### Task 5: Document Namespace Weighted Routes

**Files:**
- Modify: `README.md`
- Test: `README.md` route example by static inspection and full module tests

- [ ] **Step 1: Add README route example**

Modify `README.md` under the Nacos route configuration section to include:

````markdown
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
````

- [ ] **Step 2: Run README static checks**

Run:

```bash
rg -n "多 namespace 权重路由示例|Weight=user-service,80|metadata.discovery.namespace" README.md
```

Expected: all three patterns are found.

- [ ] **Step 3: Run full affected tests**

Run:

```bash
./gradlew :gateway-loadbalancer:test :gateway-core:test
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 4: Commit Task 5**

Run:

```bash
git add README.md
git commit -m "docs: document Nacos namespace weighted routes"
```

Expected: commit succeeds and includes only README documentation changes.

---

### Task 6: Final Verification

**Files:**
- Inspect all files changed by Tasks 1-5

- [ ] **Step 1: Run full test suite**

Run:

```bash
./gradlew test
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 2: Check formatting and whitespace**

Run:

```bash
git diff --check
```

Expected: no output and exit code 0.

- [ ] **Step 3: Confirm commits and working tree**

Run:

```bash
git status --short --branch
git log --oneline --decorate -n 8
```

Expected: implementation commits are on `main`; unrelated pre-existing local changes remain separate if they existed before this plan.

- [ ] **Step 4: Smoke-test dependency wiring without Nacos**

Run:

```bash
./gradlew :gateway-server:bootRun --args='--spring.cloud.nacos.discovery.server-addr=invalid --spring.cloud.nacos.config.server-addr=invalid' 2>&1 | head -80
```

Expected: application reaches Spring Boot startup and then fails because Nacos is unavailable; there should be no class-not-found error for `io.aegis.gateway.loadbalancer`.

- [ ] **Step 5: Final implementation summary**

Prepare a concise summary with:

- The new namespace-aware LoadBalancer files.
- The route metadata format.
- The test commands and outcomes.
- Any remaining limitation, especially that SCG `Weight` controls namespace virtual routes and does not merge multiple namespaces into one weighted instance pool.

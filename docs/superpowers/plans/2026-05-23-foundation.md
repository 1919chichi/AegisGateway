# AegisGateway Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 搭建 Gradle 多模块项目骨架，实现 gateway-core —— 所有模块的共享基础，包含 Nacos 配置同步、标准响应体、SCG 路由仓库。

**Architecture:** Spring Cloud Gateway 提供响应式路由引擎。gateway-core 是纯库模块，所有治理模块只依赖它。它拥有：通过虚拟线程监听 Nacos 配置、维护内存配置状态、标准 ApiResponse 封装、实现 SCG 的 RouteDefinitionRepository。治理模块通过 NacosConfigSyncService 注册各自的配置变更监听器，自行解析本模块配置。

**Tech Stack:** JDK 25, Gradle 8.10, Spring Boot 3.4.1, Spring Cloud Gateway 4.2.x, Spring Cloud Alibaba 2023.0.3.0 (Nacos), Jackson, JUnit 5, Mockito 5, Reactor Test

---

## File Map

### 根项目
- Create: `settings.gradle`
- Create: `build.gradle`
- Create: `.gitignore`

### gateway-core（主要工作）
- Create: `gateway-core/build.gradle`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/model/ApiResponse.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/model/AegisErrorCode.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/model/config/AegisRoute.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/model/config/AegisRoutesConfig.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/model/config/GlobalConfig.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/nacos/NacosConfigKeys.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/nacos/NacosConfigSyncService.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/route/AegisRouteDefinitionRepository.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/filter/AegisFilterOrder.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/exception/GlobalExceptionHandler.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/config/AegisCoreAutoConfiguration.java`
- Create: `gateway-core/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Test: `gateway-core/src/test/java/io/aegis/gateway/core/model/ApiResponseTest.java`
- Test: `gateway-core/src/test/java/io/aegis/gateway/core/nacos/NacosConfigSyncServiceTest.java`
- Test: `gateway-core/src/test/java/io/aegis/gateway/core/route/AegisRouteDefinitionRepositoryTest.java`
- Test: `gateway-core/src/test/java/io/aegis/gateway/core/exception/GlobalExceptionHandlerTest.java`

### gateway-server（骨架）
- Create: `gateway-server/build.gradle`
- Create: `gateway-server/src/main/java/io/aegis/gateway/server/AegisGatewayApplication.java`
- Create: `gateway-server/src/main/resources/application.yml`
- Create: `Dockerfile`

### 其他模块（本 Plan 只创建 build.gradle 骨架）
- Create: `gateway-ratelimit/build.gradle`
- Create: `gateway-circuitbreaker/build.gradle`
- Create: `gateway-loadbalancer/build.gradle`
- Create: `gateway-gray/build.gradle`
- Create: `gateway-auth/build.gradle`
- Create: `gateway-transform/build.gradle`
- Create: `gateway-mirror/build.gradle`
- Create: `gateway-admin/build.gradle`

---

## Task 1: 初始化 Gradle 多模块项目结构

**Files:**
- Create: `settings.gradle`
- Create: `build.gradle`
- Create: `.gitignore`
- Create: `gateway-core/build.gradle`
- Create: `gateway-ratelimit/build.gradle`
- Create: `gateway-circuitbreaker/build.gradle`
- Create: `gateway-loadbalancer/build.gradle`
- Create: `gateway-gray/build.gradle`
- Create: `gateway-auth/build.gradle`
- Create: `gateway-transform/build.gradle`
- Create: `gateway-mirror/build.gradle`
- Create: `gateway-admin/build.gradle`
- Create: `gateway-server/build.gradle`

- [ ] **Step 1: 创建 settings.gradle**

```groovy
rootProject.name = 'aegis-gateway'

include 'gateway-core'
include 'gateway-ratelimit'
include 'gateway-circuitbreaker'
include 'gateway-loadbalancer'
include 'gateway-gray'
include 'gateway-auth'
include 'gateway-transform'
include 'gateway-mirror'
include 'gateway-admin'
include 'gateway-server'
```

- [ ] **Step 2: 创建根 build.gradle**

```groovy
plugins {
    id 'java'
    id 'org.springframework.boot' version '3.4.1' apply false
    id 'io.spring.dependency-management' version '1.1.7' apply false
}

subprojects {
    apply plugin: 'java'
    apply plugin: 'io.spring.dependency-management'

    group = 'io.aegis.gateway'
    version = '1.0.0-SNAPSHOT'

    java {
        toolchain {
            languageVersion = JavaLanguageVersion.of(25)
        }
    }

    repositories {
        mavenCentral()
    }

    dependencyManagement {
        imports {
            mavenBom "org.springframework.boot:spring-boot-dependencies:3.4.1"
            mavenBom "org.springframework.cloud:spring-cloud-dependencies:2024.0.0"
            mavenBom "com.alibaba.cloud:spring-cloud-alibaba-dependencies:2023.0.3.0"
        }
    }

    dependencies {
        testImplementation 'org.springframework.boot:spring-boot-starter-test'
        testImplementation 'io.projectreactor:reactor-test'
    }

    test {
        useJUnitPlatform()
        jvmArgs '--enable-preview'
    }

    compileJava {
        options.compilerArgs += ['--enable-preview']
        options.release = 25
    }

    compileTestJava {
        options.compilerArgs += ['--enable-preview']
        options.release = 25
    }
}
```

- [ ] **Step 3: 创建 gateway-core/build.gradle**

```groovy
dependencies {
    implementation 'org.springframework.cloud:spring-cloud-starter-gateway'
    implementation 'com.alibaba.cloud:spring-cloud-starter-alibaba-nacos-discovery'
    implementation 'com.alibaba.cloud:spring-cloud-starter-alibaba-nacos-config'
    implementation 'com.fasterxml.jackson.core:jackson-databind'
    implementation 'org.springframework.boot:spring-boot-autoconfigure'
    compileOnly 'org.springframework.boot:spring-boot-autoconfigure-processor'
}
```

- [ ] **Step 4: 创建其他模块的 build.gradle（仅骨架）**

`gateway-ratelimit/build.gradle`:
```groovy
dependencies {
    implementation project(':gateway-core')
    implementation 'org.redisson:redisson-spring-boot-starter:3.37.0'
}
```

`gateway-circuitbreaker/build.gradle`:
```groovy
dependencies {
    implementation project(':gateway-core')
    implementation 'org.springframework.cloud:spring-cloud-starter-circuitbreaker-resilience4j'
}
```

`gateway-loadbalancer/build.gradle`:
```groovy
dependencies {
    implementation project(':gateway-core')
    implementation 'org.springframework.cloud:spring-cloud-starter-loadbalancer'
    implementation 'com.alibaba.cloud:spring-cloud-starter-alibaba-nacos-discovery'
}
```

`gateway-gray/build.gradle`, `gateway-auth/build.gradle`, `gateway-transform/build.gradle`, `gateway-mirror/build.gradle`, `gateway-admin/build.gradle`（均相同骨架）:
```groovy
dependencies {
    implementation project(':gateway-core')
}
```

`gateway-server/build.gradle`:
```groovy
plugins {
    id 'org.springframework.boot'
}

dependencies {
    implementation project(':gateway-core')
    implementation project(':gateway-ratelimit')
    implementation project(':gateway-circuitbreaker')
    implementation project(':gateway-loadbalancer')
    implementation project(':gateway-gray')
    implementation project(':gateway-auth')
    implementation project(':gateway-transform')
    implementation project(':gateway-mirror')
    implementation project(':gateway-admin')
    implementation 'org.springframework.boot:spring-boot-starter-webflux'
}
```

- [ ] **Step 5: 创建 .gitignore**

```
.gradle/
build/
*.class
*.log
.idea/
*.iml
.DS_Store
```

- [ ] **Step 6: 初始化 Gradle Wrapper 并验证项目结构**

```bash
cd /Users/cz/github-project/AegisGateway
gradle wrapper --gradle-version 8.10
./gradlew projects
```

期望输出包含所有 10 个子项目列表。

- [ ] **Step 7: 提交**

```bash
git add .
git commit -m "chore: initialize Gradle multi-module project structure"
```

---

## Task 2: 实现 ApiResponse 和 AegisErrorCode

**Files:**
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/model/ApiResponse.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/model/AegisErrorCode.java`
- Test: `gateway-core/src/test/java/io/aegis/gateway/core/model/ApiResponseTest.java`

- [ ] **Step 1: 写失败测试**

`gateway-core/src/test/java/io/aegis/gateway/core/model/ApiResponseTest.java`:
```java
package io.aegis.gateway.core.model;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class ApiResponseTest {

    @Test
    void success_shouldReturnCode200WithData() {
        ApiResponse<String> response = ApiResponse.success("hello");

        assertThat(response.code()).isEqualTo(200);
        assertThat(response.message()).isEqualTo("success");
        assertThat(response.data()).isEqualTo("hello");
        assertThat(response.timestamp()).isPositive();
    }

    @Test
    void error_withErrorCode_shouldReturnNullData() {
        ApiResponse<Void> response = ApiResponse.error(AegisErrorCode.ROUTE_NOT_FOUND);

        assertThat(response.code()).isEqualTo(40401);
        assertThat(response.message()).isEqualTo("Route not found");
        assertThat(response.data()).isNull();
        assertThat(response.timestamp()).isPositive();
    }

    @Test
    void error_withRateLimitPath_shouldReturn42901() {
        ApiResponse<Void> response = ApiResponse.error(AegisErrorCode.RATE_LIMIT_PATH);

        assertThat(response.code()).isEqualTo(42901);
    }

    @Test
    void error_withCircuitBreakerOpen_shouldReturn50300() {
        ApiResponse<Void> response = ApiResponse.error(AegisErrorCode.SERVICE_UNAVAILABLE);

        assertThat(response.code()).isEqualTo(50300);
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
cd /Users/cz/github-project/AegisGateway
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.model.ApiResponseTest"
```

期望：编译失败，`ApiResponse` 和 `AegisErrorCode` 不存在。

- [ ] **Step 3: 实现 AegisErrorCode**

`gateway-core/src/main/java/io/aegis/gateway/core/model/AegisErrorCode.java`:
```java
package io.aegis.gateway.core.model;

public enum AegisErrorCode {
    SUCCESS(200, "success"),
    BAD_REQUEST(40000, "Bad request"),
    PARAM_MISSING(40001, "Parameter missing"),
    PARAM_INVALID(40002, "Parameter invalid"),
    UNAUTHORIZED(40100, "Unauthorized"),
    API_KEY_INVALID(40101, "API key invalid"),
    FORBIDDEN(40300, "Forbidden"),
    NOT_FOUND(40400, "Not found"),
    ROUTE_NOT_FOUND(40401, "Route not found"),
    RATE_LIMIT_PATH(42901, "Rate limit exceeded (path)"),
    RATE_LIMIT_SERVICE(42902, "Rate limit exceeded (service)"),
    RATE_LIMIT_USER(42903, "Rate limit exceeded (user)"),
    INTERNAL_ERROR(50000, "Internal server error"),
    SERVICE_UNAVAILABLE(50300, "Service unavailable (circuit breaker open)");

    private final int code;
    private final String message;

    AegisErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() { return code; }
    public String getMessage() { return message; }
}
```

- [ ] **Step 4: 实现 ApiResponse**

`gateway-core/src/main/java/io/aegis/gateway/core/model/ApiResponse.java`:
```java
package io.aegis.gateway.core.model;

public record ApiResponse<T>(int code, String message, T data, long timestamp) {

    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(200, "success", data, System.currentTimeMillis());
    }

    public static <T> ApiResponse<T> error(AegisErrorCode errorCode) {
        return new ApiResponse<>(errorCode.getCode(), errorCode.getMessage(), null, System.currentTimeMillis());
    }

    public static <T> ApiResponse<T> error(int code, String message) {
        return new ApiResponse<>(code, message, null, System.currentTimeMillis());
    }
}
```

- [ ] **Step 5: 运行测试，确认通过**

```bash
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.model.ApiResponseTest"
```

期望：3 个测试全部 PASS。

- [ ] **Step 6: 提交**

```bash
git add gateway-core/src/
git commit -m "feat(core): add ApiResponse and AegisErrorCode"
```

---

## Task 3: 实现 Nacos 配置模型

**Files:**
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/model/config/AegisRoute.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/model/config/AegisRoutesConfig.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/model/config/GlobalConfig.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/nacos/NacosConfigKeys.java`
- Test: `gateway-core/src/test/java/io/aegis/gateway/core/model/config/ConfigModelDeserializationTest.java`

- [ ] **Step 1: 写失败测试（JSON 反序列化）**

`gateway-core/src/test/java/io/aegis/gateway/core/model/config/ConfigModelDeserializationTest.java`:
```java
package io.aegis.gateway.core.model.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class ConfigModelDeserializationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void shouldDeserializeAegisRoutesConfig() throws Exception {
        String json = """
            {
              "routes": [
                {
                  "id": "user-service",
                  "uri": "lb://user-service",
                  "predicates": ["Path=/api/user/**"],
                  "filters": ["StripPrefix=1"],
                  "metadata": {
                    "rateLimit": {"ruleId": "user-service-limit"},
                    "retry": {"maxAttempts": 3}
                  }
                }
              ]
            }
            """;

        AegisRoutesConfig config = objectMapper.readValue(json, AegisRoutesConfig.class);

        assertThat(config.routes()).hasSize(1);
        AegisRoute route = config.routes().get(0);
        assertThat(route.id()).isEqualTo("user-service");
        assertThat(route.uri()).isEqualTo("lb://user-service");
        assertThat(route.predicates()).containsExactly("Path=/api/user/**");
        assertThat(route.filters()).containsExactly("StripPrefix=1");
        assertThat(route.metadata()).containsKey("rateLimit");
    }

    @Test
    void shouldDeserializeGlobalConfig() throws Exception {
        String json = """
            {
              "cors": {
                "allowedOrigins": ["*"],
                "allowedMethods": ["GET", "POST"]
              },
              "auth": {
                "jwtSecret": "my-secret",
                "excludePaths": ["/api/public/**"]
              },
              "admin": {
                "apiKey": "admin-key-123"
              }
            }
            """;

        GlobalConfig config = objectMapper.readValue(json, GlobalConfig.class);

        assertThat(config.cors().allowedOrigins()).containsExactly("*");
        assertThat(config.auth().jwtSecret()).isEqualTo("my-secret");
        assertThat(config.admin().apiKey()).isEqualTo("admin-key-123");
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.model.config.ConfigModelDeserializationTest"
```

期望：编译失败，类不存在。

- [ ] **Step 3: 实现 AegisRoute**

`gateway-core/src/main/java/io/aegis/gateway/core/model/config/AegisRoute.java`:
```java
package io.aegis.gateway.core.model.config;

import java.util.List;
import java.util.Map;

public record AegisRoute(
        String id,
        String uri,
        List<String> predicates,
        List<String> filters,
        Map<String, Object> metadata
) {}
```

- [ ] **Step 4: 实现 AegisRoutesConfig**

`gateway-core/src/main/java/io/aegis/gateway/core/model/config/AegisRoutesConfig.java`:
```java
package io.aegis.gateway.core.model.config;

import java.util.List;

public record AegisRoutesConfig(List<AegisRoute> routes) {
    public static AegisRoutesConfig empty() {
        return new AegisRoutesConfig(List.of());
    }
}
```

- [ ] **Step 5: 实现 GlobalConfig**

`gateway-core/src/main/java/io/aegis/gateway/core/model/config/GlobalConfig.java`:
```java
package io.aegis.gateway.core.model.config;

import java.util.List;

public record GlobalConfig(CorsConfig cors, AuthConfig auth, AdminConfig admin) {

    public record CorsConfig(List<String> allowedOrigins, List<String> allowedMethods) {}
    public record AuthConfig(String jwtSecret, List<String> excludePaths) {}
    public record AdminConfig(String apiKey) {}

    public static GlobalConfig defaults() {
        return new GlobalConfig(
            new CorsConfig(List.of("*"), List.of("GET", "POST", "PUT", "DELETE", "OPTIONS")),
            new AuthConfig("", List.of()),
            new AdminConfig("")
        );
    }
}
```

- [ ] **Step 6: 实现 NacosConfigKeys**

`gateway-core/src/main/java/io/aegis/gateway/core/nacos/NacosConfigKeys.java`:
```java
package io.aegis.gateway.core.nacos;

public final class NacosConfigKeys {
    public static final String GROUP = "aegis";
    public static final String ROUTES = "aegis-routes.json";
    public static final String GOVERNANCE = "aegis-governance.json";
    public static final String GLOBAL = "aegis-global.json";

    private NacosConfigKeys() {}
}
```

- [ ] **Step 7: 运行测试，确认通过**

```bash
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.model.config.ConfigModelDeserializationTest"
```

期望：2 个测试全部 PASS。

- [ ] **Step 8: 提交**

```bash
git add gateway-core/src/
git commit -m "feat(core): add Nacos config models (AegisRoute, GlobalConfig)"
```

---

## Task 4: 实现 NacosConfigSyncService

**Files:**
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/nacos/NacosConfigSyncService.java`
- Test: `gateway-core/src/test/java/io/aegis/gateway/core/nacos/NacosConfigSyncServiceTest.java`

- [ ] **Step 1: 写失败测试**

`gateway-core/src/test/java/io/aegis/gateway/core/nacos/NacosConfigSyncServiceTest.java`:
```java
package io.aegis.gateway.core.nacos;

import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aegis.gateway.core.model.config.AegisRoutesConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NacosConfigSyncServiceTest {

    @Mock
    private ConfigService configService;

    private NacosConfigSyncService syncService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        when(configService.getConfig(eq(NacosConfigKeys.ROUTES), eq(NacosConfigKeys.GROUP), anyLong()))
            .thenReturn("{\"routes\":[]}");
        when(configService.getConfig(eq(NacosConfigKeys.GOVERNANCE), eq(NacosConfigKeys.GROUP), anyLong()))
            .thenReturn("{}");
        when(configService.getConfig(eq(NacosConfigKeys.GLOBAL), eq(NacosConfigKeys.GROUP), anyLong()))
            .thenReturn("{\"cors\":{\"allowedOrigins\":[\"*\"],\"allowedMethods\":[\"GET\"]},\"auth\":{\"jwtSecret\":\"\",\"excludePaths\":[]},\"admin\":{\"apiKey\":\"\"}}");

        syncService = new NacosConfigSyncService(configService, objectMapper);
        syncService.init();
    }

    @Test
    void init_shouldLoadInitialRoutesConfig() {
        AegisRoutesConfig routes = syncService.getRoutesConfig();
        assertThat(routes.routes()).isEmpty();
    }

    @Test
    void init_shouldSubscribeToNacosListeners() throws Exception {
        verify(configService, times(3)).addListener(any(), eq(NacosConfigKeys.GROUP), any(Listener.class));
    }

    @Test
    void routeChange_shouldUpdateInMemoryConfigAndNotifyListener() throws Exception {
        ArgumentCaptor<Listener> listenerCaptor = ArgumentCaptor.forClass(Listener.class);
        verify(configService).addListener(eq(NacosConfigKeys.ROUTES), eq(NacosConfigKeys.GROUP), listenerCaptor.capture());

        AtomicReference<AegisRoutesConfig> received = new AtomicReference<>();
        syncService.registerRoutesListener(received::set);

        String newRoutesJson = """
            {"routes":[{"id":"svc-a","uri":"lb://svc-a","predicates":["Path=/api/a/**"],"filters":[],"metadata":{}}]}
            """;
        listenerCaptor.getValue().receiveConfigInfo(newRoutesJson);

        // 等待虚拟线程执行
        Thread.sleep(100);

        assertThat(syncService.getRoutesConfig().routes()).hasSize(1);
        assertThat(syncService.getRoutesConfig().routes().get(0).id()).isEqualTo("svc-a");
        assertThat(received.get().routes()).hasSize(1);
    }

    @Test
    void governanceChange_shouldNotifyRegisteredListener() throws Exception {
        ArgumentCaptor<Listener> listenerCaptor = ArgumentCaptor.forClass(Listener.class);
        verify(configService).addListener(eq(NacosConfigKeys.GOVERNANCE), eq(NacosConfigKeys.GROUP), listenerCaptor.capture());

        AtomicReference<String> received = new AtomicReference<>();
        syncService.registerGovernanceListener(received::set);

        listenerCaptor.getValue().receiveConfigInfo("{\"rateLimits\":[]}");
        Thread.sleep(100);

        assertThat(received.get()).isEqualTo("{\"rateLimits\":[]}");
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.nacos.NacosConfigSyncServiceTest"
```

期望：编译失败，`NacosConfigSyncService` 不存在。

- [ ] **Step 3: 实现 NacosConfigSyncService**

`gateway-core/src/main/java/io/aegis/gateway/core/nacos/NacosConfigSyncService.java`:
```java
package io.aegis.gateway.core.nacos;

import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.AbstractListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aegis.gateway.core.model.config.AegisRoutesConfig;
import io.aegis.gateway.core.model.config.GlobalConfig;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class NacosConfigSyncService {

    private static final Logger log = LoggerFactory.getLogger(NacosConfigSyncService.class);
    private static final long TIMEOUT_MS = 5000L;

    private final ConfigService configService;
    private final ObjectMapper objectMapper;

    private final AtomicReference<AegisRoutesConfig> routesConfig =
            new AtomicReference<>(AegisRoutesConfig.empty());
    private final AtomicReference<String> governanceConfigJson =
            new AtomicReference<>("{}");
    private final AtomicReference<GlobalConfig> globalConfig =
            new AtomicReference<>(GlobalConfig.defaults());

    private final List<Consumer<AegisRoutesConfig>> routesListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<String>> governanceListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<GlobalConfig>> globalListeners = new CopyOnWriteArrayList<>();

    public NacosConfigSyncService(ConfigService configService, ObjectMapper objectMapper) {
        this.configService = configService;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() throws Exception {
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            scope.fork(() -> { loadAndSubscribeRoutes(); return null; });
            scope.fork(() -> { loadAndSubscribeGovernance(); return null; });
            scope.fork(() -> { loadAndSubscribeGlobal(); return null; });
            scope.join().throwIfFailed();
        }
    }

    private void loadAndSubscribeRoutes() throws Exception {
        String json = configService.getConfig(NacosConfigKeys.ROUTES, NacosConfigKeys.GROUP, TIMEOUT_MS);
        if (json != null) {
            updateRoutesConfig(json);
        }
        configService.addListener(NacosConfigKeys.ROUTES, NacosConfigKeys.GROUP, new AbstractListener() {
            @Override
            public void receiveConfigInfo(String configInfo) {
                Thread.ofVirtual().start(() -> updateRoutesConfig(configInfo));
            }
        });
    }

    private void loadAndSubscribeGovernance() throws Exception {
        String json = configService.getConfig(NacosConfigKeys.GOVERNANCE, NacosConfigKeys.GROUP, TIMEOUT_MS);
        if (json != null) {
            updateGovernanceConfig(json);
        }
        configService.addListener(NacosConfigKeys.GOVERNANCE, NacosConfigKeys.GROUP, new AbstractListener() {
            @Override
            public void receiveConfigInfo(String configInfo) {
                Thread.ofVirtual().start(() -> updateGovernanceConfig(configInfo));
            }
        });
    }

    private void loadAndSubscribeGlobal() throws Exception {
        String json = configService.getConfig(NacosConfigKeys.GLOBAL, NacosConfigKeys.GROUP, TIMEOUT_MS);
        if (json != null) {
            updateGlobalConfig(json);
        }
        configService.addListener(NacosConfigKeys.GLOBAL, NacosConfigKeys.GROUP, new AbstractListener() {
            @Override
            public void receiveConfigInfo(String configInfo) {
                Thread.ofVirtual().start(() -> updateGlobalConfig(configInfo));
            }
        });
    }

    private void updateRoutesConfig(String json) {
        try {
            AegisRoutesConfig config = objectMapper.readValue(json, AegisRoutesConfig.class);
            routesConfig.set(config);
            routesListeners.forEach(l -> l.accept(config));
        } catch (Exception e) {
            log.error("Failed to parse routes config", e);
        }
    }

    private void updateGovernanceConfig(String json) {
        governanceConfigJson.set(json);
        governanceListeners.forEach(l -> l.accept(json));
    }

    private void updateGlobalConfig(String json) {
        try {
            GlobalConfig config = objectMapper.readValue(json, GlobalConfig.class);
            globalConfig.set(config);
            globalListeners.forEach(l -> l.accept(config));
        } catch (Exception e) {
            log.error("Failed to parse global config", e);
        }
    }

    public AegisRoutesConfig getRoutesConfig() { return routesConfig.get(); }
    public String getGovernanceConfigJson() { return governanceConfigJson.get(); }
    public GlobalConfig getGlobalConfig() { return globalConfig.get(); }

    public void registerRoutesListener(Consumer<AegisRoutesConfig> listener) {
        routesListeners.add(listener);
    }

    public void registerGovernanceListener(Consumer<String> listener) {
        governanceListeners.add(listener);
    }

    public void registerGlobalListener(Consumer<GlobalConfig> listener) {
        globalListeners.add(listener);
    }
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.nacos.NacosConfigSyncServiceTest"
```

期望：4 个测试全部 PASS。

- [ ] **Step 5: 提交**

```bash
git add gateway-core/src/
git commit -m "feat(core): implement NacosConfigSyncService with virtual threads and structured concurrency"
```

---

## Task 5: 实现 AegisRouteDefinitionRepository

**Files:**
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/route/AegisRouteDefinitionRepository.java`
- Test: `gateway-core/src/test/java/io/aegis/gateway/core/route/AegisRouteDefinitionRepositoryTest.java`

- [ ] **Step 1: 写失败测试**

`gateway-core/src/test/java/io/aegis/gateway/core/route/AegisRouteDefinitionRepositoryTest.java`:
```java
package io.aegis.gateway.core.route;

import io.aegis.gateway.core.model.config.AegisRoute;
import io.aegis.gateway.core.model.config.AegisRoutesConfig;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.context.ApplicationEventPublisher;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AegisRouteDefinitionRepositoryTest {

    @Mock
    private NacosConfigSyncService syncService;
    @Mock
    private ApplicationEventPublisher publisher;

    private AegisRouteDefinitionRepository repository;

    @BeforeEach
    void setUp() {
        when(syncService.getRoutesConfig()).thenReturn(AegisRoutesConfig.empty());
        repository = new AegisRouteDefinitionRepository(syncService, publisher);
    }

    @Test
    void getRouteDefinitions_shouldReturnEmptyWhenNoRoutes() {
        StepVerifier.create(repository.getRouteDefinitions())
            .verifyComplete();
    }

    @Test
    void getRouteDefinitions_shouldConvertAegisRouteToRouteDefinition() {
        AegisRoute route = new AegisRoute(
            "user-service",
            "lb://user-service",
            List.of("Path=/api/user/**"),
            List.of("StripPrefix=1"),
            Map.of()
        );
        when(syncService.getRoutesConfig())
            .thenReturn(new AegisRoutesConfig(List.of(route)));
        repository = new AegisRouteDefinitionRepository(syncService, publisher);

        StepVerifier.create(repository.getRouteDefinitions())
            .assertNext(def -> {
                assertThat(def.getId()).isEqualTo("user-service");
                assertThat(def.getUri().toString()).isEqualTo("lb://user-service");
                assertThat(def.getPredicates()).hasSize(1);
                assertThat(def.getFilters()).hasSize(1);
            })
            .verifyComplete();
    }

    @Test
    void onRoutesChange_shouldRefreshRoutesAndPublishEvent() {
        ArgumentCaptor<Consumer<AegisRoutesConfig>> listenerCaptor =
            ArgumentCaptor.forClass(Consumer.class);
        verify(syncService).registerRoutesListener(listenerCaptor.capture());

        AegisRoute newRoute = new AegisRoute(
            "order-service", "lb://order-service",
            List.of("Path=/api/order/**"), List.of(), Map.of()
        );
        listenerCaptor.getValue().accept(new AegisRoutesConfig(List.of(newRoute)));

        verify(publisher).publishEvent(any());

        StepVerifier.create(repository.getRouteDefinitions())
            .assertNext(def -> assertThat(def.getId()).isEqualTo("order-service"))
            .verifyComplete();
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.route.AegisRouteDefinitionRepositoryTest"
```

期望：编译失败，`AegisRouteDefinitionRepository` 不存在。

- [ ] **Step 3: 实现 AegisRouteDefinitionRepository**

`gateway-core/src/main/java/io/aegis/gateway/core/route/AegisRouteDefinitionRepository.java`:
```java
package io.aegis.gateway.core.route;

import io.aegis.gateway.core.model.config.AegisRoute;
import io.aegis.gateway.core.model.config.AegisRoutesConfig;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import org.springframework.cloud.gateway.event.RefreshRoutesEvent;
import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.cloud.gateway.route.RouteDefinitionRepository;
import org.springframework.context.ApplicationEventPublisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AegisRouteDefinitionRepository implements RouteDefinitionRepository {

    private final Map<String, RouteDefinition> routeStore = new ConcurrentHashMap<>();
    private final ApplicationEventPublisher publisher;

    public AegisRouteDefinitionRepository(NacosConfigSyncService syncService,
                                          ApplicationEventPublisher publisher) {
        this.publisher = publisher;
        AegisRoutesConfig initial = syncService.getRoutesConfig();
        applyRoutes(initial.routes());
        syncService.registerRoutesListener(config -> {
            applyRoutes(config.routes());
            publisher.publishEvent(new RefreshRoutesEvent(this));
        });
    }

    private void applyRoutes(List<AegisRoute> routes) {
        routeStore.clear();
        routes.forEach(r -> routeStore.put(r.id(), toRouteDefinition(r)));
    }

    private RouteDefinition toRouteDefinition(AegisRoute route) {
        RouteDefinition def = new RouteDefinition();
        def.setId(route.id());
        def.setUri(URI.create(route.uri()));
        def.setPredicates(route.predicates().stream()
            .map(PredicateDefinition::new)
            .toList());
        def.setFilters(route.filters().stream()
            .map(FilterDefinition::new)
            .toList());
        def.setMetadata(route.metadata() != null ? route.metadata() : Map.of());
        return def;
    }

    @Override
    public Flux<RouteDefinition> getRouteDefinitions() {
        return Flux.fromIterable(routeStore.values());
    }

    @Override
    public Mono<Void> save(Mono<RouteDefinition> route) {
        return route.doOnNext(r -> routeStore.put(r.getId(), r)).then();
    }

    @Override
    public Mono<Void> delete(Mono<String> routeId) {
        return routeId.doOnNext(routeStore::remove).then();
    }
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.route.AegisRouteDefinitionRepositoryTest"
```

期望：3 个测试全部 PASS。

- [ ] **Step 5: 提交**

```bash
git add gateway-core/src/
git commit -m "feat(core): implement AegisRouteDefinitionRepository with Nacos-driven hot reload"
```

---

## Task 6: 实现 GlobalExceptionHandler 和 AegisFilterOrder

**Files:**
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/filter/AegisFilterOrder.java`
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/exception/GlobalExceptionHandler.java`
- Test: `gateway-core/src/test/java/io/aegis/gateway/core/exception/GlobalExceptionHandlerTest.java`

- [ ] **Step 1: 写失败测试**

`gateway-core/src/test/java/io/aegis/gateway/core/exception/GlobalExceptionHandlerTest.java`:
```java
package io.aegis.gateway.core.exception;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.aegis.gateway.core.model.ApiResponse;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalExceptionHandlerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final GlobalExceptionHandler handler = new GlobalExceptionHandler(objectMapper);

    @Test
    void handle_shouldReturnJsonApiResponseWithCode50000() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/test").build());
        var result = handler.handle(exchange, new RuntimeException("unexpected error"));

        StepVerifier.create(result).verifyComplete();

        assertThat(exchange.getResponse().getHeaders().getContentType())
            .isEqualTo(MediaType.APPLICATION_JSON);

        byte[] body = exchange.getResponse().getBodyAsString().block().getBytes();
        assertThat(body).isNotEmpty();
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.exception.GlobalExceptionHandlerTest"
```

期望：编译失败。

- [ ] **Step 3: 实现 AegisFilterOrder**

`gateway-core/src/main/java/io/aegis/gateway/core/filter/AegisFilterOrder.java`:
```java
package io.aegis.gateway.core.filter;

public final class AegisFilterOrder {
    public static final int AUTH = -200;
    public static final int RATE_LIMIT = -100;
    public static final int GRAY = -50;
    public static final int LOAD_BALANCER = 100;
    public static final int CIRCUIT_BREAKER = 200;
    public static final int RETRY = 300;
    public static final int MIRROR = 400;

    private AegisFilterOrder() {}
}
```

- [ ] **Step 4: 实现 GlobalExceptionHandler**

`gateway-core/src/main/java/io/aegis/gateway/core/exception/GlobalExceptionHandler.java`:
```java
package io.aegis.gateway.core.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aegis.gateway.core.model.AegisErrorCode;
import io.aegis.gateway.core.model.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;
import reactor.core.publisher.Mono;

@Component
@Order(-1)
public class GlobalExceptionHandler implements WebExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private static final byte[] FALLBACK_BODY =
        "{\"code\":50000,\"message\":\"Internal server error\",\"data\":null,\"timestamp\":0}".getBytes();

    private final ObjectMapper objectMapper;

    public GlobalExceptionHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        var response = exchange.getResponse();
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);

        AegisErrorCode errorCode = resolveErrorCode(ex);
        response.setStatusCode(resolveHttpStatus(ex));

        byte[] body;
        try {
            body = objectMapper.writeValueAsBytes(ApiResponse.error(errorCode));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize error response", e);
            body = FALLBACK_BODY;
        }

        log.error("Gateway exception: {}", ex.getMessage(), ex);
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
    }

    private AegisErrorCode resolveErrorCode(Throwable ex) {
        if (ex instanceof ResponseStatusException rse) {
            if (rse.getStatusCode() == HttpStatus.NOT_FOUND) return AegisErrorCode.NOT_FOUND;
            if (rse.getStatusCode() == HttpStatus.UNAUTHORIZED) return AegisErrorCode.UNAUTHORIZED;
            if (rse.getStatusCode() == HttpStatus.FORBIDDEN) return AegisErrorCode.FORBIDDEN;
            if (rse.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE) return AegisErrorCode.SERVICE_UNAVAILABLE;
        }
        return AegisErrorCode.INTERNAL_ERROR;
    }

    private HttpStatus resolveHttpStatus(Throwable ex) {
        if (ex instanceof ResponseStatusException rse && rse.getStatusCode() instanceof HttpStatus status) {
            return status;
        }
        return HttpStatus.INTERNAL_SERVER_ERROR;
    }
}
```

- [ ] **Step 5: 运行测试，确认通过**

```bash
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.exception.GlobalExceptionHandlerTest"
```

期望：1 个测试 PASS。

- [ ] **Step 6: 提交**

```bash
git add gateway-core/src/
git commit -m "feat(core): add GlobalExceptionHandler and AegisFilterOrder constants"
```

---

## Task 7: 实现 Spring Boot 自动配置

**Files:**
- Create: `gateway-core/src/main/java/io/aegis/gateway/core/config/AegisCoreAutoConfiguration.java`
- Create: `gateway-core/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`

- [ ] **Step 1: 实现 AegisCoreAutoConfiguration**

`gateway-core/src/main/java/io/aegis/gateway/core/config/AegisCoreAutoConfiguration.java`:
```java
package io.aegis.gateway.core.config;

import com.alibaba.nacos.api.config.ConfigService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aegis.gateway.core.exception.GlobalExceptionHandler;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.core.route.AegisRouteDefinitionRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.cloud.gateway.route.RouteDefinitionRepository;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
public class AegisCoreAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public NacosConfigSyncService nacosConfigSyncService(ConfigService configService,
                                                         ObjectMapper objectMapper) {
        return new NacosConfigSyncService(configService, objectMapper);
    }

    @Bean
    @Primary
    @ConditionalOnMissingBean(RouteDefinitionRepository.class)
    public AegisRouteDefinitionRepository aegisRouteDefinitionRepository(
            NacosConfigSyncService syncService,
            ApplicationEventPublisher publisher) {
        return new AegisRouteDefinitionRepository(syncService, publisher);
    }

    @Bean
    @ConditionalOnMissingBean
    public GlobalExceptionHandler globalExceptionHandler(ObjectMapper objectMapper) {
        return new GlobalExceptionHandler(objectMapper);
    }
}
```

- [ ] **Step 2: 注册自动配置**

`gateway-core/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`:
```
io.aegis.gateway.core.config.AegisCoreAutoConfiguration
```

- [ ] **Step 3: 编译验证**

```bash
./gradlew :gateway-core:compileJava
```

期望：编译成功，无错误。

- [ ] **Step 4: 运行所有 gateway-core 测试**

```bash
./gradlew :gateway-core:test
```

期望：全部 PASS。

- [ ] **Step 5: 提交**

```bash
git add gateway-core/src/
git commit -m "feat(core): add Spring Boot auto-configuration for gateway-core"
```

---

## Task 8: 创建 gateway-server 骨架和 Dockerfile

**Files:**
- Create: `gateway-server/src/main/java/io/aegis/gateway/server/AegisGatewayApplication.java`
- Create: `gateway-server/src/main/resources/application.yml`
- Create: `Dockerfile`

- [ ] **Step 1: 创建主应用类**

`gateway-server/src/main/java/io/aegis/gateway/server/AegisGatewayApplication.java`:
```java
package io.aegis.gateway.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AegisGatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(AegisGatewayApplication.class, args);
    }
}
```

- [ ] **Step 2: 创建 application.yml**

`gateway-server/src/main/resources/application.yml`:
```yaml
server:
  port: 8080

spring:
  application:
    name: aegis-gateway
  cloud:
    nacos:
      discovery:
        server-addr: ${NACOS_SERVER_ADDR:127.0.0.1:8848}
        namespace: ${NACOS_NAMESPACE:}
        group: aegis
      config:
        server-addr: ${NACOS_SERVER_ADDR:127.0.0.1:8848}
        namespace: ${NACOS_NAMESPACE:}
        group: aegis
        file-extension: json

logging:
  level:
    io.aegis.gateway: DEBUG
```

- [ ] **Step 3: 创建 Dockerfile**

`Dockerfile`（位于项目根目录）:
```dockerfile
FROM eclipse-temurin:25-jre-alpine

WORKDIR /app

COPY gateway-server/build/libs/gateway-server-*.jar app.jar

ENV NACOS_SERVER_ADDR=127.0.0.1:8848
ENV NACOS_NAMESPACE=

EXPOSE 8080

ENTRYPOINT ["java", "--enable-preview", "-jar", "app.jar"]
```

- [ ] **Step 4: 编译整个项目**

```bash
./gradlew :gateway-server:compileJava
```

期望：编译成功。

- [ ] **Step 5: 提交**

```bash
git add gateway-server/ Dockerfile
git commit -m "feat(server): add gateway-server skeleton and Dockerfile"
```

---

## Task 9: 整体冒烟测试验证

- [ ] **Step 1: 运行所有模块测试**

```bash
./gradlew test
```

期望：所有模块测试全部 PASS，无编译错误。

- [ ] **Step 2: 构建可执行 JAR**

```bash
./gradlew :gateway-server:bootJar
ls gateway-server/build/libs/
```

期望：生成 `gateway-server-1.0.0-SNAPSHOT.jar`。

- [ ] **Step 3: 验证 JAR 包含依赖**

```bash
java --enable-preview -jar gateway-server/build/libs/gateway-server-1.0.0-SNAPSHOT.jar --spring.cloud.nacos.discovery.server-addr=invalid 2>&1 | head -20
```

期望：Spring Boot 启动日志出现，失败是因为 Nacos 连接不上（这是正常的，说明主流程走通了）。

- [ ] **Step 4: 最终提交**

```bash
git add .
git commit -m "chore: foundation complete - all tests passing, bootJar builds successfully"
```

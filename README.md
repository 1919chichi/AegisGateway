# AegisGateway

**English** | [简体中文](README.zh-CN.md)

A reactive API gateway built on **Spring Cloud Gateway (WebFlux)**, with Nacos as its single configuration source. It runs on **Java 25** and makes use of virtual threads and Structured Concurrency.

## Features

- **Dynamic routing**: all route definitions live in Nacos and are refreshed in real time without restarting the gateway
- **JWT authentication**: configurable path exclusions
- **Distributed rate limiting**: policy-based Redisson/Redis rate limiting across service, path, and user dimensions, with fail-open behavior when Redis is unavailable
- **Circuit breaking**: powered by Resilience4j
- **Load balancing**: Nacos service discovery with Spring Cloud LoadBalancer, including instance-weighted consistent hashing and automatic round-robin fallback
- **Gray/canary routing**: controlled traffic splitting
- **Request/response transformation**: header rewriting and body mapping
- **Traffic mirroring**: asynchronous request mirroring to shadow services
- **Admin API**: REST endpoints for managing Nacos-backed configuration
- **Parallel configuration loading**: Java 25 Structured Concurrency loads all configuration sets in parallel at startup and fails the startup atomically if any load fails

## Modules

```text
aegis-gateway/
├── gateway-core           # Nacos sync, route repository, error handling, shared models
├── gateway-server         # The only runnable Spring Boot application; assembles all modules
├── gateway-ratelimit      # Distributed rate limiting with Redisson/Redis
├── gateway-circuitbreaker # Circuit breaking with Resilience4j
├── gateway-loadbalancer   # Namespace-aware and consistent-hash load balancing
├── gateway-gray           # Gray/canary routing
├── gateway-auth           # JWT authentication
├── gateway-transform      # Request/response transformation
├── gateway-mirror         # Traffic mirroring
├── gateway-admin          # Configuration management Admin REST API
└── namespace-demo-service # Local demo service for multi-namespace routing
```

## Technology Stack

| Component | Version | Purpose |
|---|---|---|
| Java | 25 (`--enable-preview`) | Records, virtual threads, Structured Concurrency |
| Spring Boot | 4.0.6 | Application framework |
| Spring Cloud | 2025.1.1 | Gateway, LoadBalancer, CircuitBreaker |
| Spring Cloud Alibaba | 2025.1.0.0 | Nacos service discovery and dynamic configuration |
| Nacos Client | 3.1.1 | Configuration listeners and service registration |
| Redisson | 4.4.0 | Redis client for distributed rate limiting |
| Resilience4j | 2.3.0 | Circuit breaking |
| Project Reactor | _Managed by the Spring Boot BOM_ | End-to-end reactive processing |

## Quick Start

### Prerequisites

- JDK 25+
- Docker Compose, for running Nacos and Redis locally

### Start Local Infrastructure

```bash
docker compose up -d nacos redis
```

Default endpoints:

| Service | Address |
|---|---|
| Nacos API | `127.0.0.1:8848` |
| Nacos console | `http://127.0.0.1:18080/` |
| Redis | `127.0.0.1:6379` |

### Build

Build the executable Spring Boot JAR:

```bash
./gradlew :gateway-server:bootJar
```

Or run the gateway directly through Gradle. The build config supplies the required Java 25 preview flags:

```bash
./gradlew :gateway-server:bootRun
```

### Run the JAR

```bash
java --enable-preview -jar gateway-server/build/libs/gateway-server-*.jar
```

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `NACOS_SERVER_ADDR` | `127.0.0.1:8848` | Nacos server address |
| `NACOS_NAMESPACE` | _(empty)_ | Nacos namespace |
| `AEGIS_NACOS_GROUP` | `aegis` | Nacos group for all Aegis configuration |

### Docker

```bash
# Build the JAR before building the image
./gradlew :gateway-server:bootJar
docker build -t aegis-gateway .

# Run the image
docker run -p 8080:8080 \
  -e NACOS_SERVER_ADDR=<nacos-host>:8848 \
  -e AEGIS_NACOS_GROUP=aegis \
  aegis-gateway
```

## Nacos Configuration

The gateway reads three Data IDs. Their group is controlled by `AEGIS_NACOS_GROUP` and defaults to `aegis`.

| Data ID | Format | Description |
|---|---|---|
| `aegis-routes.json` | JSON | Route definitions |
| `aegis-governance.json` | JSON | Governance settings parsed by modules such as rate limiting and load balancing |
| `aegis-global.json` | JSON | Global CORS, JWT secret, and Admin API key settings |

### Route Example (`aegis-routes.json`)

```json
{
  "routes": [
    {
      "id": "user-service",
      "uri": "lb://user-service",
      "predicates": ["Path=/api/users/**"],
      "filters": ["StripPrefix=1"],
      "order": 0,
      "metadata": {}
    }
  ]
}
```

### Weighted Multi-Namespace Routing

A service name can be represented by multiple virtual routes. The Spring Cloud Gateway `Weight` predicate controls traffic distribution between namespaces. This example sends `/api/users/**` traffic to the `dev` and `gray` namespaces at an 80:20 ratio while both routes still target `lb://user-service`.

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

`Weight` only selects a virtual route. Once selected, `gateway-loadbalancer` discovers healthy instances exclusively from the namespace specified by that route's `metadata.discovery.namespace`.

### Consistent-Hash Load Balancing (`aegis-governance.json`)

Enable weighted consistent hashing per Nacos `serviceId`, using either a request header or the client IP as the session-affinity key:

```json
{
  "loadBalancePolicies": [
    {
      "serviceId": "order-service",
      "strategy": "CONSISTENT_HASH",
      "keySource": "HEADER",
      "keyName": "X-User-Id",
      "virtualNodesPerWeight": 160
    }
  ]
}
```

`keySource` supports `HEADER` and `CLIENT_IP`. `CLIENT_IP` reads the first address from `X-Forwarded-For`, so the upstream proxy must set that header correctly. `virtualNodesPerWeight` defaults to 160 and combines with each Nacos instance weight to determine its virtual-node count. If no policy exists, the hash key cannot be extracted, or consistent-hash routing fails, the gateway fails open to standard round-robin load balancing.

### Rate Limiting (`aegis-routes.json` + `aegis-governance.json`)

Rate limiting follows a **policy-binding** model. A route references a policy group through `metadata.rateLimit.policyId`, while the actual rules live in `aegis-governance.json`. Rules are hot-reloaded from Nacos, so changing limits does not require editing route definitions.

Bind a route to a policy:

```json
{
  "routes": [
    {
      "id": "user-service",
      "uri": "lb://user-service",
      "predicates": ["Path=/api/users/**"],
      "filters": ["StripPrefix=1"],
      "metadata": {
        "rateLimit": {
          "policyId": "user-service-policy"
        }
      }
    }
  ]
}
```

Define the Redis connection and policy rules in the governance configuration:

```json
{
  "rateLimitRedis": {
    "address": "redis://127.0.0.1:6379",
    "password": null,
    "database": 0
  },
  "rateLimitPolicies": [
    {
      "id": "user-service-policy",
      "rules": [
        {
          "id": "user-service-total",
          "type": "SERVICE",
          "capacity": 1000,
          "refillRate": 500
        },
        {
          "id": "user-login-path",
          "type": "PATH",
          "pathPattern": "/api/users/login",
          "capacity": 50,
          "refillRate": 10
        },
        {
          "id": "user-api-per-user",
          "type": "USER",
          "capacity": 60,
          "refillRate": 10,
          "identityHeader": "X-User-Id"
        }
      ]
    }
  ]
}
```

Each rule is a Redis token bucket implemented with Lua and shared by all gateway instances. `capacity` controls burst size; `refillRate` is the number of tokens added per second and approximates steady-state QPS.

| `type` | Meaning | Match condition |
|---|---|---|
| `SERVICE` | Limits total traffic to the downstream service | Every request to a route bound to the policy |
| `PATH` | Limits a URL pattern using Spring `PathPattern` syntax | Original request path matches `pathPattern` |
| `USER` | Limits each user, identified by `identityHeader` (default `X-User-Id`) | Every request, with missing identities sharing an anonymous bucket |

All matched rules use **AND semantics**: a request passes only when every matching bucket grants a token. Otherwise the gateway returns `429` with the unified `ApiResponse` and a type-specific error code:

| Failed rule type | Error code |
|---|---|
| `PATH` | `42901` |
| `SERVICE` | `42902` |
| `USER` | `42903` |

**Fail-open startup isolation**: rate limiting is protective infrastructure, not a new single point of failure. Requests pass when a route has no binding, a policy is missing, `rateLimitRedis` is absent, Redis is unavailable, or token acquisition fails. Redis connections are created lazily only when rate-limiting policies exist, and rate limiting recovers automatically after Redis becomes available again.

See the [rate-limit policy design](docs/superpowers/specs/2026-06-11-redisson-rate-limit-policy-design.md) for key design, multi-rule deduction boundaries, and hot-reload details.

### Global Configuration (`aegis-global.json`)

```json
{
  "cors": {
    "allowedOrigins": ["https://example.com"],
    "allowedMethods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
  },
  "auth": {
    "jwtSecret": "your-secret-key",
    "excludePaths": ["/api/public/**", "/actuator/health"]
  },
  "admin": {
    "apiKey": "your-admin-api-key"
  }
}
```

> **Note:** route creation and deletion must go through the Admin API and Nacos. Calling the Spring Cloud Gateway route repository directly is unsupported; `save` and `delete` throw `UnsupportedOperationException`.

## Filter Order

```text
AUTH (-200) → RATE_LIMIT (-100) → GRAY (-50) → EXCEPTION_HANDLER (-2)
  → [built-in SCG filters]
  → CIRCUIT_BREAKER (10050) → RETRY (10300) → MIRROR (10400)
```

## Route Update Lifecycle

```text
Nacos pushes a change
  → NacosConfigSyncService deserializes it
  → AegisRouteDefinitionRepository atomically replaces the in-memory route map
  → RefreshRoutesEvent is published
  → Spring Cloud Gateway reloads routes
```

## Testing

```bash
# Run the complete test suite
./gradlew test

# Test one module
./gradlew :gateway-core:test

# Run one test class
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.route.AegisRouteDefinitionRepositoryTest"
```

## Adding a Feature Module

1. Create the module directory and add `implementation project(':gateway-core')` to its `build.gradle`.
2. Register it with `include 'gateway-<name>'` in `settings.gradle`.
3. Add `implementation project(':gateway-<name>')` to `gateway-server/build.gradle`.
4. Implement a `GlobalFilter` bean using the appropriate order constant from `AegisFilterOrder`.
5. For Nacos-backed configuration, register a listener through `NacosConfigSyncService.registerGovernanceListener()` or `registerGlobalListener()`.

## License

This project is provided for learning and reference purposes only.

# Redisson Rate Limit Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `gateway-ratelimit` module so routes bind a `policyId`, governance config defines multiple rate-limit rules, and matched rules are enforced through reactive Redisson.

**Architecture:** `RateLimitPolicyRepository` owns parsing and hot-updated policy snapshots from `NacosConfigSyncService`. `RateLimitFilter` reads the matched SCG route metadata, computes matching rules, and delegates Redis bucket operations behind a small reactive gateway so tests can exercise behavior without Redis. `RateLimitKeyResolver` keeps Redis key generation deterministic and safe.

**Tech Stack:** Java 25, Spring Cloud Gateway WebFlux, Redisson Reactive API, Reactor, Jackson 3, JUnit 6, Mockito.

---

### Task 1: Policy Model And Repository

**Files:**
- Create: `gateway-ratelimit/src/main/java/io/aegis/gateway/ratelimit/model/RateLimitType.java`
- Create: `gateway-ratelimit/src/main/java/io/aegis/gateway/ratelimit/model/RateLimitRule.java`
- Create: `gateway-ratelimit/src/main/java/io/aegis/gateway/ratelimit/model/RateLimitPolicy.java`
- Create: `gateway-ratelimit/src/main/java/io/aegis/gateway/ratelimit/model/RateLimitGovernanceConfig.java`
- Create: `gateway-ratelimit/src/main/java/io/aegis/gateway/ratelimit/repository/RateLimitPolicyRepository.java`
- Test: `gateway-ratelimit/src/test/java/io/aegis/gateway/ratelimit/repository/RateLimitPolicyRepositoryTest.java`

- [x] **Step 1: Write failing tests for valid parsing, invalid JSON retention, and invalid rule retention**
- [x] **Step 2: Run repository tests and verify they fail because classes do not exist**
- [x] **Step 3: Implement records, enum, validation, listener registration, and snapshot lookup**
- [x] **Step 4: Run repository tests and verify they pass**

### Task 2: Key Resolver And Rule Matching

**Files:**
- Create: `gateway-ratelimit/src/main/java/io/aegis/gateway/ratelimit/core/RateLimitKeyResolver.java`
- Create: `gateway-ratelimit/src/main/java/io/aegis/gateway/ratelimit/core/MatchedRateLimitRule.java`
- Test: `gateway-ratelimit/src/test/java/io/aegis/gateway/ratelimit/core/RateLimitKeyResolverTest.java`

- [x] **Step 1: Write failing tests for service, path, user, sanitize, truncation, and rule order**
- [x] **Step 2: Run key resolver tests and verify they fail because classes do not exist**
- [x] **Step 3: Implement PathPattern matching, serviceId extraction, identity resolution, safe keys, and USER -> PATH -> SERVICE ordering**
- [x] **Step 4: Run key resolver tests and verify they pass**

### Task 3: Reactive Limiter Gateway

**Files:**
- Create: `gateway-ratelimit/src/main/java/io/aegis/gateway/ratelimit/core/ReactiveRateLimiterGateway.java`
- Create: `gateway-ratelimit/src/main/java/io/aegis/gateway/ratelimit/core/RedissonReactiveRateLimiterGateway.java`
- Test: `gateway-ratelimit/src/test/java/io/aegis/gateway/ratelimit/core/RedissonReactiveRateLimiterGatewayTest.java`

- [x] **Step 1: Write failing tests for Lua token bucket capacity/refill mapping, denied token, and fail-open error signaling**
- [x] **Step 2: Run limiter gateway tests and verify they fail because implementation does not exist**
- [x] **Step 3: Implement Redisson reactive adapter using `RedissonReactiveClient.getScript()` and Lua token bucket**
- [x] **Step 4: Run limiter gateway tests and verify they pass**

### Task 4: Gateway Filter And Auto Configuration

**Files:**
- Create: `gateway-ratelimit/src/main/java/io/aegis/gateway/ratelimit/filter/RateLimitFilter.java`
- Create: `gateway-ratelimit/src/main/java/io/aegis/gateway/ratelimit/config/RateLimitAutoConfiguration.java`
- Create: `gateway-ratelimit/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Test: `gateway-ratelimit/src/test/java/io/aegis/gateway/ratelimit/filter/RateLimitFilterTest.java`
- Test: `gateway-ratelimit/src/test/java/io/aegis/gateway/ratelimit/config/RateLimitAutoConfigurationTest.java`

- [x] **Step 1: Write failing tests for pass-through, missing policy fail-open, all-rules-pass, first-failed-rule 429 response, Redis error fail-open, and filter order**
- [x] **Step 2: Run filter tests and verify they fail because implementation does not exist**
- [x] **Step 3: Implement route metadata lookup, matched rule evaluation, response writing with `ApiResponse`, and auto-configuration**
- [x] **Step 4: Run filter and auto-configuration tests and verify they pass**

### Task 5: Verification

**Files:**
- Modify only files from Tasks 1-4 if verification reveals defects.

- [x] **Step 1: Run `./gradlew :gateway-ratelimit:test`**
- [x] **Step 2: Run `./gradlew test` to catch cross-module regressions**
- [x] **Step 3: Check `git status --short --branch` and summarize changed files**

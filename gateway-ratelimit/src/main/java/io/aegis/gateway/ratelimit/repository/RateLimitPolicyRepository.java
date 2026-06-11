package io.aegis.gateway.ratelimit.repository;

import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.ratelimit.core.RedissonClientManager;
import io.aegis.gateway.ratelimit.model.RateLimitGovernanceConfig;
import io.aegis.gateway.ratelimit.model.RateLimitPolicy;
import io.aegis.gateway.ratelimit.model.RateLimitRedisConfig;
import io.aegis.gateway.ratelimit.model.RateLimitRule;
import io.aegis.gateway.ratelimit.model.RateLimitType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.util.pattern.PathPatternParser;
import tools.jackson.databind.ObjectMapper;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 监听 Nacos 治理配置中的 {@code rateLimitPolicies} 与 {@code rateLimitRedis} 节点，
 * 维护限流策略的内存快照，并驱动 {@link RedissonClientManager} 的客户端生命周期。
 * <p>
 * 初始快照由 {@link NacosConfigSyncService} 在注册监听器时回放，不要在构造器里
 * 自行 get 初始值——那种模式与并发到达的 Nacos 推送存在新值被旧快照覆盖的竞态。
 * <p>
 * 配置整体校验失败（JSON 非法、规则非法、Redis 地址非法）时保留旧快照且不触碰
 * 客户端状态，坏配置不会打掉正在生效的限流。
 */
public class RateLimitPolicyRepository {

    private static final Logger log = LoggerFactory.getLogger(RateLimitPolicyRepository.class);

    private final ObjectMapper objectMapper;
    private final RedissonClientManager clientManager;
    private final AtomicReference<Map<String, RateLimitPolicy>> policies = new AtomicReference<>(Map.of());
    private final PathPatternParser pathPatternParser = new PathPatternParser();

    public RateLimitPolicyRepository(NacosConfigSyncService syncService,
                                     ObjectMapper objectMapper,
                                     RedissonClientManager clientManager) {
        this.objectMapper = objectMapper;
        this.clientManager = clientManager;
        syncService.registerGovernanceListener(this::onGovernanceUpdate);
    }

    public Optional<RateLimitPolicy> findById(String policyId) {
        if (policyId == null || policyId.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(policies.get().get(policyId));
    }

    private void onGovernanceUpdate(String json) {
        try {
            RateLimitGovernanceConfig config = objectMapper.readValue(json, RateLimitGovernanceConfig.class);
            validate(config);
            Map<String, RateLimitPolicy> snapshot = config.rateLimitPolicies().stream()
                    .collect(Collectors.toUnmodifiableMap(RateLimitPolicy::id, Function.identity()));
            policies.set(snapshot);
            // 快照生效后再调整客户端：无策略时不保持 Redis 连接（"配置了限流才使用 Redis"）
            clientManager.apply(config.rateLimitRedis(), !snapshot.isEmpty());
        } catch (Exception e) {
            log.error("Failed to parse rate limit policies, keep previous snapshot", e);
        }
    }

    private void validate(RateLimitGovernanceConfig config) {
        Set<String> policyIds = new HashSet<>();
        for (RateLimitPolicy policy : config.rateLimitPolicies()) {
            requireText(policy.id(), "rate limit policy id must not be blank");
            if (!policyIds.add(policy.id())) {
                throw new IllegalArgumentException("Duplicate rate limit policy id: " + policy.id());
            }
            Set<String> ruleIds = new HashSet<>();
            for (RateLimitRule rule : policy.rules()) {
                validateRule(policy.id(), rule, ruleIds);
            }
        }
        validateRedisConfig(config.rateLimitRedis());
    }

    private void validateRule(String policyId, RateLimitRule rule, Set<String> ruleIds) {
        requireText(rule.id(), "rate limit rule id must not be blank");
        if (!ruleIds.add(rule.id())) {
            throw new IllegalArgumentException("Duplicate rate limit rule id under policy " + policyId + ": " + rule.id());
        }
        if (rule.type() == null) {
            throw new IllegalArgumentException("rate limit rule type must not be null: " + rule.id());
        }
        if (rule.capacity() <= 0 || rule.refillRate() <= 0) {
            throw new IllegalArgumentException("rate limit capacity/refillRate must be positive: " + rule.id());
        }
        if (rule.type() == RateLimitType.PATH) {
            requireText(rule.pathPattern(), "PATH rate limit rule pathPattern must not be blank: " + rule.id());
            pathPatternParser.parse(rule.pathPattern());
        }
    }

    private static void validateRedisConfig(RateLimitRedisConfig redisConfig) {
        // rateLimitRedis 可以缺失（限流整体 fail-open），但写了就必须是合法地址，
        // 防止配置笔误悄悄把限流降级成 fail-open
        if (redisConfig != null && !redisConfig.hasValidAddress()) {
            throw new IllegalArgumentException(
                    "rateLimitRedis.address must start with redis:// or rediss://, got: " + redisConfig.address());
        }
    }

    private static void requireText(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
    }
}

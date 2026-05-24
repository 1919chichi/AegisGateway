package io.aegis.gateway.gray.filter;

import io.aegis.gateway.core.filter.AegisFilterOrder;
import io.aegis.gateway.core.model.AegisDiscoveryMetadata;
import io.aegis.gateway.core.model.config.AegisRoute;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import io.aegis.gateway.gray.matcher.GrayMatcher;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 基于请求头的动态灰度路由 filter。
 * <p>
 * 从 {@code aegis-governance.json} 的 {@code governanceKey} 节点加载 {@link GrayConfig}，
 * 并在配置更新时预编译规则的匹配器，避免热路径上的对象分配；
 * 从 {@code aegis-routes.json} 构建 routeId → {@link AegisDiscoveryMetadata} 快照。
 * 每个请求逐条匹配已编译规则，命中后将目标路由的服务发现坐标写入 exchange attribute，
 * 由 {@code gateway-loadbalancer} 在实例选择时优先读取。
 * <p>
 * 灰度路由要求 targetRouteId 引用的路由在 {@code metadata.discovery} 中显式声明
 * {@code namespace} 和 {@code group}，不支持从 {@link com.alibaba.cloud.nacos.NacosDiscoveryProperties}
 * 继承默认值——灰度覆盖的语义是"明确路由到不同命名空间"，未声明则视为该路由无灰度坐标（记录 warn 日志并直通）。
 */
public class GrayRoutingFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(GrayRoutingFilter.class);

    /**
     * 预编译的匹配器与目标路由 ID 对，在配置更新时原子替换，避免热路径 toMatcher() 分配。
     */
    private record CompiledRule(GrayMatcher matcher, String targetRouteId) {}

    private final String governanceKey;
    private final ObjectMapper objectMapper;
    private final AtomicReference<List<CompiledRule>> compiledRules = new AtomicReference<>(List.of());
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
                compiledRules.set(List.of());
                return;
            }
            GrayConfig config = objectMapper.treeToValue(node, GrayConfig.class);
            List<CompiledRule> compiled = new ArrayList<>();
            for (GrayRule rule : config.rules()) {
                compiled.add(new CompiledRule(rule.toMatcher(), rule.targetRouteId()));
            }
            compiledRules.set(List.copyOf(compiled));
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
        for (CompiledRule rule : compiledRules.get()) {
            if (rule.matcher().matches(exchange)) {
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

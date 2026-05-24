package io.aegis.gateway.core.model.config;

import java.util.List;
import java.util.Map;

/**
 * Nacos {@code aegis-routes.json} 中单条路由的配置模型。
 * <p>
 * {@code predicates} 和 {@code filters} 均使用 Spring Cloud Gateway 的文本 DSL 格式，
 * 例如 {@code "Path=/api/**"} 或 {@code "StripPrefix=1"}。
 * {@code metadata} 透传至 SCG 的 RouteDefinition；若包含键 {@code "discovery"}，
 * 负载均衡器将从中读取 Nacos 命名空间/分组覆盖值（见 AegisDiscoveryMetadata）。
 */
public record AegisRoute(
        String id,
        String uri,
        List<String> predicates,
        List<String> filters,
        int order,
        Map<String, Object> metadata
) {
    public AegisRoute {
        predicates = predicates == null ? List.of() : List.copyOf(predicates);
        filters    = filters    == null ? List.of() : List.copyOf(filters);
        metadata   = metadata   == null ? Map.of()  : Map.copyOf(metadata);
    }
}

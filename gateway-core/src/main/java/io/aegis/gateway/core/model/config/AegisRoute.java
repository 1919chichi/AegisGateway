package io.aegis.gateway.core.model.config;

import java.util.List;
import java.util.Map;

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

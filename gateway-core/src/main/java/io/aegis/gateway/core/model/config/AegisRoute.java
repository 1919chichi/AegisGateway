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
) {}

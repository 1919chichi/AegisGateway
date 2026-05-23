package io.aegis.gateway.core.model.config;

import java.util.List;

public record AegisRoutesConfig(List<AegisRoute> routes) {
    public AegisRoutesConfig {
        routes = routes == null ? List.of() : List.copyOf(routes);
    }

    public static AegisRoutesConfig empty() {
        return new AegisRoutesConfig(List.of());
    }
}

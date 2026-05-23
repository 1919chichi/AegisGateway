package io.aegis.gateway.core.model.config;

import java.util.List;

public record GlobalConfig(CorsConfig cors, AuthConfig auth, AdminConfig admin) {

    public record CorsConfig(List<String> allowedOrigins, List<String> allowedMethods) {
        public CorsConfig {
            allowedOrigins = allowedOrigins == null ? List.of() : List.copyOf(allowedOrigins);
            allowedMethods = allowedMethods == null ? List.of() : List.copyOf(allowedMethods);
        }
    }

    public record AuthConfig(String jwtSecret, List<String> excludePaths) {
        public AuthConfig {
            excludePaths = excludePaths == null ? List.of() : List.copyOf(excludePaths);
        }
    }

    public record AdminConfig(String apiKey) {}

    public static GlobalConfig defaults() {
        return new GlobalConfig(
            new CorsConfig(List.of("*"), List.of("GET", "POST", "PUT", "DELETE", "OPTIONS")),
            new AuthConfig(null, List.of()),
            new AdminConfig("")
        );
    }
}

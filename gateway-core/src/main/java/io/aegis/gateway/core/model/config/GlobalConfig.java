package io.aegis.gateway.core.model.config;

import java.util.List;

/**
 * 从 Nacos {@code aegis-global.json} 加载的顶级网关配置。
 * 配置变更热生效，无需重启。
 */
public record GlobalConfig(CorsConfig cors, AuthConfig auth, AdminConfig admin) {

    /** CORS 跨域配置，{@code allowedOrigins} 为空时不限制来源（等效于 "*"）。 */
    public record CorsConfig(List<String> allowedOrigins, List<String> allowedMethods) {
        public CorsConfig {
            allowedOrigins = allowedOrigins == null ? List.of() : List.copyOf(allowedOrigins);
            allowedMethods = allowedMethods == null ? List.of() : List.copyOf(allowedMethods);
        }
    }

    /**
     * JWT 认证配置。{@code excludePaths} 中的路径模式跳过认证 Filter，
     * 适用于公开端点（如健康检查、登录接口）。
     */
    public record AuthConfig(String jwtSecret, List<String> excludePaths) {
        public AuthConfig {
            excludePaths = excludePaths == null ? List.of() : List.copyOf(excludePaths);
        }
    }

    /** Admin REST API 的访问密钥，通过 HTTP Header 传递（见 gateway-admin 模块）。 */
    public record AdminConfig(String apiKey) {}

    public static GlobalConfig defaults() {
        return new GlobalConfig(
            new CorsConfig(List.of("*"), List.of("GET", "POST", "PUT", "DELETE", "OPTIONS")),
            new AuthConfig(null, List.of()),
            new AdminConfig("")
        );
    }
}

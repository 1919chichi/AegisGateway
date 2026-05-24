package io.aegis.gateway.core.nacos;

/**
 * AegisGateway 在 Nacos 中使用的 Data ID 常量。
 * <p>
 * 三个 Data ID 共用同一个 Nacos Group（默认 "aegis"，可通过
 * {@code aegis.gateway.nacos.group} 配置项覆盖）。
 * <ul>
 *   <li>{@link #ROUTES} — 路由定义，对应 {@code AegisRoutesConfig}</li>
 *   <li>{@link #GOVERNANCE} — 治理配置（限流、熔断等），以原始 JSON 字符串分发给各功能模块</li>
 *   <li>{@link #GLOBAL} — 全局配置（CORS、JWT 密钥、Admin API Key），对应 {@code GlobalConfig}</li>
 * </ul>
 */
public final class NacosConfigKeys {
    public static final String DEFAULT_GROUP = "aegis";
    public static final String ROUTES = "aegis-routes.json";
    public static final String GOVERNANCE = "aegis-governance.json";
    public static final String GLOBAL = "aegis-global.json";

    private NacosConfigKeys() {}
}

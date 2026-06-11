package io.aegis.gateway.ratelimit.model;

/**
 * 限流使用的 Redis 连接配置，来自 {@code aegis-governance.json} 的 {@code rateLimitRedis} 节点。
 * <p>
 * Redis 地址放在 Nacos 治理配置而非 application.yml，与"Nacos 是配置唯一来源"的项目约定
 * 保持一致，同时让 Redis 连接支持热更新：record 的 equals 即为变更指纹，
 * {@code RedissonClientManager} 据此判断是否需要重建客户端。
 *
 * @param address  Redis 地址，必须为 {@code redis://host:port} 或 {@code rediss://host:port} 格式
 * @param password 密码，null 或空白表示无密码
 * @param database 逻辑库编号，缺省归一化为 0（Jackson 3 对缺失的 primitive 字段默认报错，故用包装类型）
 */
public record RateLimitRedisConfig(String address, String password, Integer database) {

    public RateLimitRedisConfig {
        database = database == null ? 0 : database;
    }

    /** Redisson 单机模式要求的地址前缀，校验时使用。 */
    public static final String REDIS_SCHEME = "redis://";
    public static final String REDIS_TLS_SCHEME = "rediss://";

    public boolean hasValidAddress() {
        return address != null
                && (address.startsWith(REDIS_SCHEME) || address.startsWith(REDIS_TLS_SCHEME));
    }
}

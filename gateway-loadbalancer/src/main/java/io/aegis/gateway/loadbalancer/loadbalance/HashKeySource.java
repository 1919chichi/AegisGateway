package io.aegis.gateway.loadbalancer.loadbalance;

/**
 * 一致性哈希路由 key 的来源。第一版只支持这两种，不支持 query 参数或 Cookie
 * （见设计文档 Non-Goals）。
 */
public enum HashKeySource {
    /** 取客户端 IP，经 {@code X-Forwarded-For} 请求头识别；网关前没有反向代理写入该头时无法取值，触发降级。 */
    CLIENT_IP,
    /** 取指定请求头的值，header 名由 {@link LoadBalancePolicy#keyName()} 指定，该来源下 keyName 必填。 */
    HEADER
}

package io.aegis.gateway.core.filter;

public final class AegisFilterOrder {
    public static final int AUTH = -200;
    public static final int RATE_LIMIT = -100;
    public static final int GRAY = -50;
    public static final int CIRCUIT_BREAKER = 10050;
    // SCG ReactiveLoadBalancerClientFilter 使用 10150；Aegis 通过 Spring Cloud LoadBalancer 扩展实例选择，不定义自有 LB GlobalFilter。
    public static final int RETRY = 10300;
    public static final int MIRROR = 10400;

    private AegisFilterOrder() {}
}

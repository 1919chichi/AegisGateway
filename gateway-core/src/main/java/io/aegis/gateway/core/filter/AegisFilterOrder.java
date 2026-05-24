package io.aegis.gateway.core.filter;

/**
 * 定义所有 Aegis 网关 Filter 的执行顺序。
 * <p>
 * Spring Cloud Gateway 以整数 order 值排序 Filter，数值越小越先执行。
 * 负值在 SCG 内置 Filter 之前运行，正值在其之后运行。
 * 认证、限流、灰度路由需要在路由决策前介入，因此使用负值。
 * 熔断和重试需要包裹上游调用，因此在负载均衡解析出具体实例地址后运行。
 */
public final class AegisFilterOrder {

    /** 捕获所有未处理异常，将其转换为结构化的 ApiResponse JSON 响应。 */
    public static final int EXCEPTION_HANDLER = -2;

    /** 在任何路由或功能评估之前完成 JWT 令牌校验。 */
    public static final int AUTH = -200;

    /** 在转发请求前执行路径/服务/用户维度的限流检查。 */
    public static final int RATE_LIMIT = -100;

    /** 在路由选择前应用金丝雀/灰度路由的请求头逻辑。 */
    public static final int GRAY = -50;

    /** 对上游调用施加熔断保护。 */
    public static final int CIRCUIT_BREAKER = 10050;

    // 必须在 SCG ReactiveLoadBalancerClientFilter（10150）之前执行，将 lb:// 替换为具体实例 URI，
    // 使 SCG 默认 LB filter 检测到 URI 已解析后自动跳过，避免重复负载均衡。
    public static final int LOAD_BALANCER = 10100;

    /** 对失败的上游调用进行重试，此时 URI 已由 LOAD_BALANCER 解析为具体实例地址。 */
    public static final int RETRY = 10300;

    /** 将请求的副本镜像到影子上游，采用 fire-and-forget 方式，不阻塞主链路。 */
    public static final int MIRROR = 10400;

    private AegisFilterOrder() {}
}

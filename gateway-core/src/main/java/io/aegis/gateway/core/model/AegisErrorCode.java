package io.aegis.gateway.core.model;

/**
 * 所有 ApiResponse 响应体使用的业务错误码。
 * <p>
 * 编码规则：前三位与 HTTP 状态码对应（如 400xx → HTTP 400，429xx → HTTP 429），
 * 后两位区分同一 HTTP 状态下的具体子场景，方便客户端细化处理逻辑。
 */
public enum AegisErrorCode {
    SUCCESS(200, "success"),
    BAD_REQUEST(40000, "Bad request"),
    PARAM_MISSING(40001, "Parameter missing"),
    PARAM_INVALID(40002, "Parameter invalid"),
    UNAUTHORIZED(40100, "Unauthorized"),
    /** Admin API Key 无效，与普通 JWT 未认证（UNAUTHORIZED）区分。 */
    API_KEY_INVALID(40101, "API key invalid"),
    FORBIDDEN(40300, "Forbidden"),
    NOT_FOUND(40400, "Not found"),
    /** 请求路径在网关路由表中不存在，与上游资源 404 区分。 */
    ROUTE_NOT_FOUND(40401, "Route not found"),
    RATE_LIMIT_PATH(42901, "Rate limit exceeded (path)"),
    RATE_LIMIT_SERVICE(42902, "Rate limit exceeded (service)"),
    RATE_LIMIT_USER(42903, "Rate limit exceeded (user)"),
    INTERNAL_ERROR(50000, "Internal server error"),
    /** 熔断器处于 OPEN 状态，上游服务不可用。 */
    SERVICE_UNAVAILABLE(50300, "Service unavailable (circuit breaker open)");

    private final int code;
    private final String message;

    AegisErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() { return code; }
    public String getMessage() { return message; }
}

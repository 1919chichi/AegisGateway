package io.aegis.gateway.core.model;

public enum AegisErrorCode {
    SUCCESS(200, "success"),
    BAD_REQUEST(40000, "Bad request"),
    PARAM_MISSING(40001, "Parameter missing"),
    PARAM_INVALID(40002, "Parameter invalid"),
    UNAUTHORIZED(40100, "Unauthorized"),
    API_KEY_INVALID(40101, "API key invalid"),
    FORBIDDEN(40300, "Forbidden"),
    NOT_FOUND(40400, "Not found"),
    ROUTE_NOT_FOUND(40401, "Route not found"),
    RATE_LIMIT_PATH(42901, "Rate limit exceeded (path)"),
    RATE_LIMIT_SERVICE(42902, "Rate limit exceeded (service)"),
    RATE_LIMIT_USER(42903, "Rate limit exceeded (user)"),
    INTERNAL_ERROR(50000, "Internal server error"),
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

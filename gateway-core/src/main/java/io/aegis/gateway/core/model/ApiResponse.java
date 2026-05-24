package io.aegis.gateway.core.model;

/**
 * 所有 HTTP 响应体的统一包装格式，包括错误响应。
 * <p>
 * {@code code} 是 {@link AegisErrorCode} 定义的业务码，并非 HTTP 状态码。
 * {@code timestamp} 是响应构建时的 epoch 毫秒数，用于客户端调试和日志关联。
 */
public record ApiResponse<T>(int code, String message, T data, long timestamp) {

    public ApiResponse {
        java.util.Objects.requireNonNull(message, "message must not be null");
    }

    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(AegisErrorCode.SUCCESS.getCode(), AegisErrorCode.SUCCESS.getMessage(), data, System.currentTimeMillis());
    }

    public static <T> ApiResponse<T> error(AegisErrorCode errorCode) {
        return new ApiResponse<>(errorCode.getCode(), errorCode.getMessage(), null, System.currentTimeMillis());
    }

    public static <T> ApiResponse<T> error(int code, String message) {
        return new ApiResponse<>(code, message, null, System.currentTimeMillis());
    }
}

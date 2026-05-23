package io.aegis.gateway.core.model;

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

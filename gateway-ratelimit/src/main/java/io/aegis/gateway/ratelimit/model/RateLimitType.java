package io.aegis.gateway.ratelimit.model;

import io.aegis.gateway.core.model.AegisErrorCode;

/**
 * 限流维度。每个维度自己持有对应的 429 业务错误码：新增维度时编译器强制同时给出错误码，
 * 避免映射散落在 Filter 的 switch 里被遗漏。
 */
public enum RateLimitType {
    SERVICE(AegisErrorCode.RATE_LIMIT_SERVICE),
    PATH(AegisErrorCode.RATE_LIMIT_PATH),
    USER(AegisErrorCode.RATE_LIMIT_USER);

    private final AegisErrorCode errorCode;

    RateLimitType(AegisErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    /** 该维度限流失败时写入 429 响应的业务错误码。 */
    public AegisErrorCode errorCode() {
        return errorCode;
    }
}

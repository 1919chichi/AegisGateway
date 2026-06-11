package io.aegis.gateway.core.exception;

import io.aegis.gateway.core.filter.AegisFilterOrder;
import io.aegis.gateway.core.model.AegisErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

/**
 * 响应式链路中未捕获异常的最终兜底处理器。
 * <p>
 * 运行于 {@link AegisFilterOrder#EXCEPTION_HANDLER}（在 SCG 内置 Filter 之前），
 * 将所有异常统一映射为业务错误码后交由 {@link ApiErrorResponseWriter} 写出，
 * 保证客户端无论异常来源都能收到结构一致的错误响应体。
 * <p>
 * 注意：429 的 {@link ResponseStatusException} 在这里只能退化为 {@link AegisErrorCode#RATE_LIMIT_PATH}，
 * 无法区分限流维度——因此限流 Filter 必须自行写出响应，不得抛异常走本处理器。
 */
@Order(AegisFilterOrder.EXCEPTION_HANDLER)
public class GlobalExceptionHandler implements WebExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    private final ObjectMapper objectMapper;

    public GlobalExceptionHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        log.error("Gateway exception: {}", ex.getMessage(), ex);
        return ApiErrorResponseWriter.write(
                exchange.getResponse(), resolveHttpStatus(ex), resolveErrorCode(ex), objectMapper);
    }

    private AegisErrorCode resolveErrorCode(Throwable ex) {
        if (ex instanceof ResponseStatusException rse) {
            if (rse.getStatusCode() == HttpStatus.BAD_REQUEST) return AegisErrorCode.BAD_REQUEST;
            if (rse.getStatusCode() == HttpStatus.UNAUTHORIZED) return AegisErrorCode.UNAUTHORIZED;
            if (rse.getStatusCode() == HttpStatus.FORBIDDEN) return AegisErrorCode.FORBIDDEN;
            if (rse.getStatusCode() == HttpStatus.NOT_FOUND) return AegisErrorCode.NOT_FOUND;
            if (rse.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS) return AegisErrorCode.RATE_LIMIT_PATH;
            if (rse.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE) return AegisErrorCode.SERVICE_UNAVAILABLE;
        }
        return AegisErrorCode.INTERNAL_ERROR;
    }

    private HttpStatus resolveHttpStatus(Throwable ex) {
        if (ex instanceof ResponseStatusException rse && rse.getStatusCode() instanceof HttpStatus status) {
            return status;
        }
        return HttpStatus.INTERNAL_SERVER_ERROR;
    }
}

package io.aegis.gateway.core.exception;

import io.aegis.gateway.core.model.AegisErrorCode;
import io.aegis.gateway.core.model.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

/**
 * 将 {@link AegisErrorCode} 以统一的 {@link ApiResponse} JSON 写出到响应的共享工具。
 * <p>
 * 网关中所有"由 Filter 直接写出错误响应"的场景（如限流 429）和 {@link GlobalExceptionHandler}
 * 都必须经由此类，保证客户端收到的错误结构在任何路径下都一致；新增字段或调整格式只需改这一处。
 * <p>
 * 若 JSON 序列化本身失败，降级为手工拼接的 JSON 字节，确保不会返回空响应体。
 * 降级路径直接字符串拼接是安全的：{@link AegisErrorCode} 的 message 均为静态 ASCII 文本，
 * 不含需要转义的字符。
 */
public final class ApiErrorResponseWriter {

    private static final Logger log = LoggerFactory.getLogger(ApiErrorResponseWriter.class);

    private ApiErrorResponseWriter() {
    }

    /**
     * 设置状态码与 Content-Type 并写出错误响应体，返回的 Mono 完成即写出结束。
     */
    public static Mono<Void> write(ServerHttpResponse response, HttpStatus status,
                                   AegisErrorCode errorCode, ObjectMapper objectMapper) {
        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);

        byte[] body;
        try {
            body = objectMapper.writeValueAsBytes(ApiResponse.error(errorCode));
        } catch (JacksonException e) {
            log.error("Failed to serialize error response", e);
            body = ("{\"code\":" + errorCode.getCode() + ",\"message\":\"" + errorCode.getMessage()
                    + "\",\"data\":null,\"timestamp\":" + System.currentTimeMillis() + "}")
                    .getBytes(StandardCharsets.UTF_8);
        }
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
    }
}

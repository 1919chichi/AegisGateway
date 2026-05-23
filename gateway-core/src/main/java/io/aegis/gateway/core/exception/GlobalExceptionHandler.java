package io.aegis.gateway.core.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aegis.gateway.core.model.AegisErrorCode;
import io.aegis.gateway.core.model.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;
import reactor.core.publisher.Mono;

@Component
@Order(-2)
public class GlobalExceptionHandler implements WebExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private static final byte[] FALLBACK_BODY =
        "{\"code\":50000,\"message\":\"Internal server error\",\"data\":null,\"timestamp\":0}".getBytes();

    private final ObjectMapper objectMapper;

    public GlobalExceptionHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        var response = exchange.getResponse();
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);

        AegisErrorCode errorCode = resolveErrorCode(ex);
        response.setStatusCode(resolveHttpStatus(ex));

        byte[] body;
        try {
            body = objectMapper.writeValueAsBytes(ApiResponse.error(errorCode));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize error response", e);
            body = FALLBACK_BODY;
        }

        log.error("Gateway exception: {}", ex.getMessage(), ex);
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
    }

    private AegisErrorCode resolveErrorCode(Throwable ex) {
        if (ex instanceof ResponseStatusException rse) {
            if (rse.getStatusCode() == HttpStatus.NOT_FOUND) return AegisErrorCode.NOT_FOUND;
            if (rse.getStatusCode() == HttpStatus.UNAUTHORIZED) return AegisErrorCode.UNAUTHORIZED;
            if (rse.getStatusCode() == HttpStatus.FORBIDDEN) return AegisErrorCode.FORBIDDEN;
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

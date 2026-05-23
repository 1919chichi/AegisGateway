package io.aegis.gateway.core.exception;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalExceptionHandlerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final GlobalExceptionHandler handler = new GlobalExceptionHandler(objectMapper);

    @Test
    void handle_shouldReturnJsonWithCode50000ForUnknownException() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/test").build());
        var result = handler.handle(exchange, new RuntimeException("unexpected error"));

        StepVerifier.create(result).verifyComplete();

        assertThat(exchange.getResponse().getHeaders().getContentType())
            .isEqualTo(MediaType.APPLICATION_JSON);
    }
}

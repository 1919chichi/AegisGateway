package io.aegis.gateway.core.exception;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.test.StepVerifier;
import tools.jackson.databind.ObjectMapper;

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

    @Test
    void handle_shouldReturn40401ForNotFoundResponseStatusException() throws Exception {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/test").build());
        var ex = new org.springframework.web.server.ResponseStatusException(
            org.springframework.http.HttpStatus.NOT_FOUND, "not found");

        StepVerifier.create(handler.handle(exchange, ex)).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode())
            .isEqualTo(org.springframework.http.HttpStatus.NOT_FOUND);
    }

    @Test
    void handle_shouldReturn42901ForTooManyRequestsException() throws Exception {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/test").build());
        var ex = new org.springframework.web.server.ResponseStatusException(
            org.springframework.http.HttpStatus.TOO_MANY_REQUESTS, "rate limited");

        StepVerifier.create(handler.handle(exchange, ex)).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode())
            .isEqualTo(org.springframework.http.HttpStatus.TOO_MANY_REQUESTS);
    }
}

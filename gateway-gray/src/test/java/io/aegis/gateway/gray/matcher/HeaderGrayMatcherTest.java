package io.aegis.gateway.gray.matcher;

import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HeaderGrayMatcherTest {

    @Test
    void matches_shouldReturnTrueWhenHeaderValueMatches() {
        HeaderGrayMatcher matcher = new HeaderGrayMatcher("X-User-Type", "beta");
        assertThat(matcher.matches(exchange("X-User-Type", "beta"))).isTrue();
    }

    @Test
    void matches_shouldReturnFalseWhenHeaderValueDiffers() {
        HeaderGrayMatcher matcher = new HeaderGrayMatcher("X-User-Type", "beta");
        assertThat(matcher.matches(exchange("X-User-Type", "stable"))).isFalse();
    }

    @Test
    void matches_shouldReturnFalseWhenHeaderAbsent() {
        HeaderGrayMatcher matcher = new HeaderGrayMatcher("X-User-Type", "beta");
        assertThat(matcher.matches(exchange("X-Other-Header", "beta"))).isFalse();
    }

    @Test
    void matches_shouldBeCaseSensitive() {
        HeaderGrayMatcher matcher = new HeaderGrayMatcher("X-User-Type", "beta");
        assertThat(matcher.matches(exchange("X-User-Type", "Beta"))).isFalse();
    }

    @Test
    void constructor_shouldThrowWhenKeyIsNull() {
        assertThatThrownBy(() -> new HeaderGrayMatcher(null, "beta"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructor_shouldThrowWhenValueIsNull() {
        assertThatThrownBy(() -> new HeaderGrayMatcher("X-User-Type", null))
                .isInstanceOf(NullPointerException.class);
    }

    private static ServerWebExchange exchange(String headerName, String headerValue) {
        return MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/test").header(headerName, headerValue).build());
    }
}

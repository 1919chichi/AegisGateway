package io.aegis.gateway.gray.matcher;

import org.springframework.web.server.ServerWebExchange;

import java.util.Objects;

/** 精确匹配请求头 key=value 的灰度匹配器。 */
public record HeaderGrayMatcher(String key, String value) implements GrayMatcher {

    public HeaderGrayMatcher {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
    }

    @Override
    public boolean matches(ServerWebExchange exchange) {
        return value.equals(exchange.getRequest().getHeaders().getFirst(key));
    }
}

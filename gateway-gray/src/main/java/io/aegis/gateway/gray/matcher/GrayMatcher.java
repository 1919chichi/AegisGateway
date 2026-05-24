package io.aegis.gateway.gray.matcher;

import org.springframework.web.server.ServerWebExchange;

/**
 * 灰度路由匹配策略接口。
 * <p>
 * 新增匹配类型只需实现此接口，并在 {@code GrayRule.toMatcher()} 的 switch 中注册，
 * 不需要修改 filter 主流程。
 */
public interface GrayMatcher {
    boolean matches(ServerWebExchange exchange);
}

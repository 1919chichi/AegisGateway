package io.aegis.gateway.gray.model;

import io.aegis.gateway.gray.matcher.GrayMatcher;
import io.aegis.gateway.gray.matcher.HeaderGrayMatcher;

/**
 * 单条灰度路由规则。
 *
 * @param type          匹配器类型，当前支持 {@code "header"}
 * @param key           匹配的请求头名称
 * @param value         期望的请求头值（精确匹配）
 * @param targetRouteId 命中时使用哪条路由的 {@code metadata.discovery} 作为服务发现坐标
 */
public record GrayRule(String type, String key, String value, String targetRouteId) {

    public GrayMatcher toMatcher() {
        return switch (type) {
            case "header" -> new HeaderGrayMatcher(key, value);
            default -> throw new IllegalArgumentException("Unknown gray rule type: " + type);
        };
    }
}

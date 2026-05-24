package io.aegis.gateway.gray.model;

import java.util.List;

/** 从 {@code aegis-governance.json} 反序列化的灰度路由配置，持有有序规则列表，第一条命中的规则生效。 */
public record GrayConfig(List<GrayRule> rules) {
    public GrayConfig {
        rules = rules == null ? List.of() : List.copyOf(rules);
    }
}

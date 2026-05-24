package io.aegis.gateway.core.model;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Objects;

/**
 * Nacos 服务发现坐标（命名空间 + 分组），同时作为 exchange attribute 的跨模块共享契约。
 * <p>
 * 路由可在 {@code aegis-routes.json} 的 {@code metadata.discovery} 中静态声明坐标；
 * {@code gateway-gray} 在运行时将命中灰度规则的请求坐标写入 exchange attribute（key = {@link #ATTR_KEY}），
 * 供 {@code gateway-loadbalancer} 动态覆盖静态路由配置。
 */
public record AegisDiscoveryMetadata(String namespace, String group) {

    private static final Logger log = LoggerFactory.getLogger(AegisDiscoveryMetadata.class);

    /**
     * 写入 {@code ServerWebExchange.attributes} 时使用的 key，用类名保证唯一且 IDE 可追踪。
     */
    public static final String ATTR_KEY = AegisDiscoveryMetadata.class.getName();
    public static final String DISCOVERY_METADATA_KEY = "discovery";
    public static final String NAMESPACE_KEY = "namespace";
    public static final String GROUP_KEY = "group";
    /** Nacos 默认分组名，未指定 group 时使用。 */
    public static final String DEFAULT_GROUP = "DEFAULT_GROUP";

    public AegisDiscoveryMetadata {
        namespace = Objects.toString(namespace, "");
        group = StringUtils.hasText(group) ? group : DEFAULT_GROUP;
    }

    public static AegisDiscoveryMetadata from(Route route, NacosDiscoveryProperties defaults) {
        AegisDiscoveryMetadata fallback = fromDefaults(defaults);
        if (route == null) {
            return fallback;
        }

        Object discovery = route.getMetadata().get(DISCOVERY_METADATA_KEY);
        if (!(discovery instanceof Map<?, ?> discoveryMap)) {
            if (discovery != null) {
                log.warn("Route {} discovery metadata must be a map, actual={}", route.getId(), discovery.getClass().getName());
            }
            return fallback;
        }

        return new AegisDiscoveryMetadata(
                stringValue(route, discoveryMap, NAMESPACE_KEY, fallback.namespace()),
                stringValue(route, discoveryMap, GROUP_KEY, fallback.group()));
    }

    public static AegisDiscoveryMetadata fromDefaults(NacosDiscoveryProperties defaults) {
        return new AegisDiscoveryMetadata(defaults.getNamespace(), defaults.getGroup());
    }

    private static String stringValue(Route route, Map<?, ?> map, String key, String fallback) {
        Object value = map.get(key);
        if (value instanceof String text && StringUtils.hasText(text)) {
            return text;
        }
        if (value != null) {
            log.warn("Route {} discovery metadata key {} must be a non-blank string, actual={}",
                    route.getId(), key, value.getClass().getName());
        }
        return fallback;
    }
}

package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Objects;

public record AegisDiscoveryMetadata(String namespace, String group) {

    private static final Logger log = LoggerFactory.getLogger(AegisDiscoveryMetadata.class);

    public static final String DISCOVERY_METADATA_KEY = "discovery";
    public static final String NAMESPACE_KEY = "namespace";
    public static final String GROUP_KEY = "group";
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

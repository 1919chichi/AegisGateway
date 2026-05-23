package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.route.Route;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AegisDiscoveryMetadataTest {

    @Test
    void from_shouldReadNamespaceAndGroupFromRouteMetadata() {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        Route route = routeWithDiscovery(Map.of(
                "namespace", "gray",
                "group", "GROUP_A"));

        AegisDiscoveryMetadata metadata = AegisDiscoveryMetadata.from(route, defaults);

        assertThat(metadata.namespace()).isEqualTo("gray");
        assertThat(metadata.group()).isEqualTo("GROUP_A");
    }

    @Test
    void from_shouldFallbackToDefaultsWhenDiscoveryMetadataIsMissing() {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        Route route = Route.async()
                .id("user-service-default")
                .uri("lb://user-service")
                .predicate(exchange -> true)
                .build();

        AegisDiscoveryMetadata metadata = AegisDiscoveryMetadata.from(route, defaults);

        assertThat(metadata.namespace()).isEqualTo("public");
        assertThat(metadata.group()).isEqualTo("DEFAULT_GROUP");
    }

    @Test
    void from_shouldFallbackToDefaultsWhenDiscoveryMetadataHasInvalidTypes() {
        NacosDiscoveryProperties defaults = defaults("public", "DEFAULT_GROUP");
        Route route = routeWithDiscovery(Map.of(
                "namespace", 123,
                "group", true));

        AegisDiscoveryMetadata metadata = AegisDiscoveryMetadata.from(route, defaults);

        assertThat(metadata.namespace()).isEqualTo("public");
        assertThat(metadata.group()).isEqualTo("DEFAULT_GROUP");
    }

    @Test
    void from_shouldUseDefaultGroupWhenDefaultGroupIsBlank() {
        NacosDiscoveryProperties defaults = defaults("", "");
        Route route = routeWithDiscovery(Map.of("namespace", "gray"));

        AegisDiscoveryMetadata metadata = AegisDiscoveryMetadata.from(route, defaults);

        assertThat(metadata.namespace()).isEqualTo("gray");
        assertThat(metadata.group()).isEqualTo("DEFAULT_GROUP");
    }

    private static NacosDiscoveryProperties defaults(String namespace, String group) {
        NacosDiscoveryProperties properties = new NacosDiscoveryProperties();
        properties.setNamespace(namespace);
        properties.setGroup(group);
        return properties;
    }

    private static Route routeWithDiscovery(Map<String, Object> discovery) {
        return Route.async()
                .id("user-service-gray")
                .uri("lb://user-service")
                .predicate(exchange -> true)
                .metadata(Map.of("discovery", discovery))
                .build();
    }
}

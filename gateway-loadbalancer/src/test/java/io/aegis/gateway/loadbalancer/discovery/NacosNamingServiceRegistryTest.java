package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.naming.NamingService;
import io.aegis.gateway.core.model.AegisDiscoveryMetadata;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NacosNamingServiceRegistryTest {

    @Test
    void getNamingService_shouldCreateServiceWithRequestedNamespace() {
        NacosDiscoveryProperties defaults = discoveryProperties();
        List<Properties> createdProperties = new ArrayList<>();
        NamingService namingService = mock(NamingService.class);
        NacosNamingServiceRegistry registry = new NacosNamingServiceRegistry(defaults, properties -> {
            createdProperties.add(properties);
            return namingService;
        });

        NamingService result = registry.getNamingService(new AegisDiscoveryMetadata("gray", "GROUP_A"));

        assertThat(result).isSameAs(namingService);
        assertThat(createdProperties).hasSize(1);
        assertThat(createdProperties.getFirst().getProperty(PropertyKeyConst.SERVER_ADDR)).isEqualTo("127.0.0.1:8848");
        assertThat(createdProperties.getFirst().getProperty(PropertyKeyConst.NAMESPACE)).isEqualTo("gray");
    }

    @Test
    void getNamingService_shouldReuseServiceForSameNamespace() {
        NacosDiscoveryProperties defaults = discoveryProperties();
        NamingService namingService = mock(NamingService.class);
        NacosNamingServiceRegistry registry = new NacosNamingServiceRegistry(defaults, properties -> namingService);

        NamingService first = registry.getNamingService(new AegisDiscoveryMetadata("gray", "GROUP_A"));
        NamingService second = registry.getNamingService(new AegisDiscoveryMetadata("gray", "GROUP_B"));

        assertThat(first).isSameAs(second);
    }

    @Test
    void shutdown_shouldCloseCreatedServices() throws Exception {
        NacosDiscoveryProperties defaults = discoveryProperties();
        NamingService grayService = mock(NamingService.class);
        NamingService devService = mock(NamingService.class);
        NacosNamingServiceRegistry registry = new NacosNamingServiceRegistry(defaults, properties -> {
            String namespace = properties.getProperty(PropertyKeyConst.NAMESPACE);
            return "gray".equals(namespace) ? grayService : devService;
        });

        registry.getNamingService(new AegisDiscoveryMetadata("gray", "DEFAULT_GROUP"));
        registry.getNamingService(new AegisDiscoveryMetadata("dev", "DEFAULT_GROUP"));
        registry.shutdown();

        verify(grayService).shutDown();
        verify(devService).shutDown();
    }

    private static NacosDiscoveryProperties discoveryProperties() {
        NacosDiscoveryProperties properties = mock(NacosDiscoveryProperties.class);
        Properties nacosProperties = new Properties();
        nacosProperties.put(PropertyKeyConst.SERVER_ADDR, "127.0.0.1:8848");
        nacosProperties.put(PropertyKeyConst.NAMESPACE, "public");
        when(properties.getNacosProperties()).thenReturn(nacosProperties);
        return properties;
    }
}

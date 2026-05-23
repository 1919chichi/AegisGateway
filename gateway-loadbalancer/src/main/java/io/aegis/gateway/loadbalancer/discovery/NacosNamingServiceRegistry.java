package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NacosNamingServiceRegistry {

    private static final Logger log = LoggerFactory.getLogger(NacosNamingServiceRegistry.class);

    private final NacosDiscoveryProperties discoveryProperties;
    private final NamingServiceFactory factory;
    private final ConcurrentMap<String, NamingService> namingServices = new ConcurrentHashMap<>();

    public NacosNamingServiceRegistry(NacosDiscoveryProperties discoveryProperties) {
        this(discoveryProperties, NamingFactory::createNamingService);
    }

    NacosNamingServiceRegistry(NacosDiscoveryProperties discoveryProperties, NamingServiceFactory factory) {
        this.discoveryProperties = discoveryProperties;
        this.factory = factory;
    }

    public NamingService getNamingService(AegisDiscoveryMetadata metadata) {
        return namingServices.computeIfAbsent(metadata.namespace(), this::createNamingService);
    }

    private NamingService createNamingService(String namespace) {
        Properties properties = new Properties();
        properties.putAll(discoveryProperties.getNacosProperties());
        properties.put(PropertyKeyConst.NAMESPACE, namespace);
        try {
            return factory.create(properties);
        } catch (NacosException e) {
            throw new IllegalStateException("Failed to create Nacos NamingService for namespace " + namespace, e);
        }
    }

    @PreDestroy
    public void shutdown() {
        namingServices.forEach((namespace, namingService) -> {
            try {
                namingService.shutDown();
            } catch (NacosException e) {
                log.warn("Failed to shut down Nacos NamingService for namespace {}", namespace, e);
            }
        });
        namingServices.clear();
    }

    @FunctionalInterface
    interface NamingServiceFactory {
        NamingService create(Properties properties) throws NacosException;
    }
}

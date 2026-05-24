package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import io.aegis.gateway.core.model.AegisDiscoveryMetadata;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 按 Nacos 命名空间缓存并复用 {@link NamingService} 实例。
 * <p>
 * 创建 {@link NamingService} 的成本较高（建立长连接），因此首次请求某命名空间时
 * 延迟创建并永久缓存，后续请求直接复用。应用关闭时统一释放所有连接。
 * <p>
 * 内部提供包级可见的构造器，用于在单元测试中注入 {@link NamingServiceFactory} mock，
 * 避免测试依赖真实 Nacos。
 */
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

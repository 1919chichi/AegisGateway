package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.cloud.nacos.NacosServiceInstance;
import com.alibaba.cloud.nacos.discovery.NacosServiceDiscovery;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestData;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR;

public class NamespaceAwareNacosServiceInstanceListSupplier implements ServiceInstanceListSupplier {

    public static final String AEGIS_NACOS_NAMESPACE = "aegis.nacos.namespace";
    public static final String AEGIS_NACOS_GROUP = "aegis.nacos.group";

    private static final Logger log = LoggerFactory.getLogger(NamespaceAwareNacosServiceInstanceListSupplier.class);

    private final String serviceId;
    private final NacosDiscoveryProperties discoveryProperties;
    private final NacosNamingServiceRegistry namingServiceRegistry;

    public NamespaceAwareNacosServiceInstanceListSupplier(String serviceId,
                                                          NacosDiscoveryProperties discoveryProperties,
                                                          NacosNamingServiceRegistry namingServiceRegistry) {
        this.serviceId = serviceId;
        this.discoveryProperties = discoveryProperties;
        this.namingServiceRegistry = namingServiceRegistry;
    }

    @Override
    public String getServiceId() {
        return serviceId;
    }

    @Override
    public Flux<List<ServiceInstance>> get() {
        return queryInstances(null);
    }

    @Override
    public Flux<List<ServiceInstance>> get(Request request) {
        return queryInstances(request);
    }

    private Flux<List<ServiceInstance>> queryInstances(Request request) {
        return Mono.fromCallable(() -> loadInstances(request))
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(error -> {
                    log.warn("Failed to query Nacos instances for serviceId={}", serviceId, error);
                    return Mono.just(List.of());
                })
                .flux();
    }

    private List<ServiceInstance> loadInstances(Request request) throws Exception {
        Route route = extractRoute(request);
        AegisDiscoveryMetadata metadata = AegisDiscoveryMetadata.from(route, discoveryProperties);
        NamingService namingService = namingServiceRegistry.getNamingService(metadata);
        List<Instance> instances = namingService.selectInstances(serviceId, metadata.group(), true);
        List<ServiceInstance> serviceInstances = instances.stream()
                .map(instance -> toServiceInstance(instance, metadata))
                .flatMap(List::stream)
                .toList();
        if (log.isDebugEnabled()) {
            log.debug("Loaded Nacos instances routeId={}, serviceId={}, namespace={}, group={}, count={}",
                    route == null ? "" : route.getId(),
                    serviceId,
                    metadata.namespace(),
                    metadata.group(),
                    serviceInstances.size());
        }
        return serviceInstances;
    }

    private Route extractRoute(Request request) {
        if (request == null || !(request.getContext() instanceof RequestDataContext context)) {
            return null;
        }
        RequestData requestData = context.getClientRequest();
        if (requestData == null) {
            return null;
        }
        Object route = requestData.getAttributes().get(GATEWAY_ROUTE_ATTR);
        return route instanceof Route selectedRoute ? selectedRoute : null;
    }

    private List<ServiceInstance> toServiceInstance(Instance instance, AegisDiscoveryMetadata metadata) {
        ServiceInstance serviceInstance = NacosServiceDiscovery.hostToServiceInstance(instance, serviceId);
        if (!(serviceInstance instanceof NacosServiceInstance nacosServiceInstance)) {
            return List.of();
        }
        Map<String, String> enrichedMetadata = new HashMap<>(nacosServiceInstance.getMetadata());
        enrichedMetadata.put(AEGIS_NACOS_NAMESPACE, metadata.namespace());
        enrichedMetadata.put(AEGIS_NACOS_GROUP, metadata.group());
        nacosServiceInstance.setMetadata(enrichedMetadata);
        return List.of(nacosServiceInstance);
    }
}

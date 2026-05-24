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

/**
 * 支持按路由覆盖 Nacos 命名空间/分组的 ServiceInstanceListSupplier 实现。
 * <p>
 * 从当前请求匹配到的 SCG Route 中读取 {@link AegisDiscoveryMetadata}，
 * 决定向哪个命名空间的 Nacos 查询服务实例。未配置覆盖时使用应用级默认值。
 * <p>
 * Nacos SDK 的实例查询是阻塞调用，因此订阅在 {@code Schedulers.boundedElastic()} 上执行，
 * 避免阻塞 Reactor 事件循环线程。查询失败时降级返回空列表，由上层熔断/重试机制处理。
 * <p>
 * 查询结果中每个 ServiceInstance 会被注入 {@link #AEGIS_NACOS_NAMESPACE} 和
 * {@link #AEGIS_NACOS_GROUP} 两个 metadata 条目，供下游 Filter 或日志使用。
 */
public class NamespaceAwareNacosServiceInstanceListSupplier implements ServiceInstanceListSupplier {

    /** 注入到 ServiceInstance metadata 中，记录该实例所属的 Nacos 命名空间。 */
    public static final String AEGIS_NACOS_NAMESPACE = "aegis.nacos.namespace";
    /** 注入到 ServiceInstance metadata 中，记录该实例所属的 Nacos 分组。 */
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

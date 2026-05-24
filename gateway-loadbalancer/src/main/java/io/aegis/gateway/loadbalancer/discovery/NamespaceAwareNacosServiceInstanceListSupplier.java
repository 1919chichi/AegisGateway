package io.aegis.gateway.loadbalancer.discovery;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.cloud.nacos.NacosServiceInstance;
import com.alibaba.cloud.nacos.discovery.NacosServiceDiscovery;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import io.aegis.gateway.core.model.AegisDiscoveryMetadata;
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
 * 支持按命名空间/分组覆盖的 Nacos ServiceInstanceListSupplier 实现。
 * <p>
 * 查询实例时按以下优先级确定目标命名空间和分组：
 * <ol>
 *   <li>Exchange attribute（{@link AegisDiscoveryMetadata#ATTR_KEY}，由灰度 Filter 写入）</li>
 *   <li>SCG Route metadata（{@code metadata.discovery}，由路由配置静态指定）</li>
 *   <li>应用级默认值（{@link NacosDiscoveryProperties}）</li>
 * </ol>
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
        // 优先读取 GrayRoutingFilter 写入的 exchange attribute，再回退到路由 metadata / 全局默认值
        AegisDiscoveryMetadata attrMetadata = extractGrayMetadata(request);
        AegisDiscoveryMetadata metadata = attrMetadata != null
                ? attrMetadata
                : AegisDiscoveryMetadata.from(extractRoute(request), discoveryProperties);
        NamingService namingService = namingServiceRegistry.getNamingService(metadata);
        List<Instance> instances = namingService.selectInstances(serviceId, metadata.group(), true);
        List<ServiceInstance> serviceInstances = instances.stream()
                .map(instance -> toServiceInstance(instance, metadata))
                .flatMap(List::stream)
                .toList();
        if (log.isDebugEnabled()) {
            log.debug("Loaded Nacos instances serviceId={}, namespace={}, group={}, count={}",
                    serviceId, metadata.namespace(), metadata.group(), serviceInstances.size());
        }
        return serviceInstances;
    }

    private Route extractRoute(Request request) {
        Map<String, Object> attributes = extractAttributes(request);
        if (attributes == null) {
            return null;
        }
        Object route = attributes.get(GATEWAY_ROUTE_ATTR);
        return route instanceof Route selectedRoute ? selectedRoute : null;
    }

    private AegisDiscoveryMetadata extractGrayMetadata(Request request) {
        Map<String, Object> attributes = extractAttributes(request);
        if (attributes == null) {
            return null;
        }
        Object attr = attributes.get(AegisDiscoveryMetadata.ATTR_KEY);
        return attr instanceof AegisDiscoveryMetadata m ? m : null;
    }

    /**
     * 从 LB Request 中提取 exchange attributes，消除两处调用方的重复解包逻辑。
     * 返回 {@code null} 表示 request 为空或上下文类型不符，调用方应按缺失情况处理。
     */
    private Map<String, Object> extractAttributes(Request request) {
        if (request == null || !(request.getContext() instanceof RequestDataContext context)) {
            return null;
        }
        RequestData requestData = context.getClientRequest();
        return requestData == null ? null : requestData.getAttributes();
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

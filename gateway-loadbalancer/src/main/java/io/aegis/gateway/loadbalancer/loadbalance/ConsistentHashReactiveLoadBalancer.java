package io.aegis.gateway.loadbalancer.loadbalance;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.aegis.gateway.loadbalancer.hash.ConsistentHashRing;
import io.aegis.gateway.loadbalancer.hash.DefaultHashKeyExtractor;
import io.aegis.gateway.loadbalancer.hash.HashKeyExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.DefaultResponse;
import org.springframework.cloud.client.loadbalancer.EmptyResponse;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.loadbalancer.core.ReactorServiceInstanceLoadBalancer;
import org.springframework.cloud.loadbalancer.core.RoundRobinLoadBalancer;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 按 serviceId 装配的一致性哈希 {@link ReactorServiceInstanceLoadBalancer}。
 * <p>
 * 必须实现这个更具体的子接口而非上层的 {@code ReactiveLoadBalancer}——Spring Cloud Gateway 的
 * {@code ReactiveLoadBalancerClientFilter} 是按
 * {@code clientFactory.getInstance(serviceId, ReactorServiceInstanceLoadBalancer.class)} 取 Bean 的，
 * 只实现上层接口会导致这个查找永远匹配不到本类，SCG 默认的 {@code RoundRobinLoadBalancer} 会被
 * 静默保留使用，且没有任何报错（见 final-review C1）。
 * <p>
 * 内部持有一个 {@link RoundRobinLoadBalancer}（构造方式与 Spring Cloud LoadBalancer 自身
 * 创建默认实例完全一致）作为降级委托：serviceId 未配置一致性哈希 policy、或请求提取不到
 * 哈希 key 时，都直接委托给它，不额外引入分支逻辑，"未配置 policy" 和 "key 缺失降级"
 * 两条需求由同一个委托对象满足。
 * <p>
 * 环用 Caffeine {@code Cache<String, ConsistentHashRing>}（{@code maximumSize(2)}）
 * 按"实例集合 + 权重 + 虚拟节点数"拼成的字符串做缓存 key：只需要保留"当前"和"上一个"两份
 * 即可覆盖切换瞬间的并发读，{@code get(key, loader)} 语义天然线程安全（命中直接返回、
 * 未命中调用 loader 构建），不需要手写 {@code AtomicReference} + 签名比较 + 原子替换的
 * 样板代码。
 * <p>
 * Nacos 实例查询本身已经在 {@code NamespaceAwareNacosServiceInstanceListSupplier} 内部
 * 跑在 {@code boundedElastic} 上，这一层不需要额外调度。
 */
public class ConsistentHashReactiveLoadBalancer implements ReactorServiceInstanceLoadBalancer {

    private static final Logger log = LoggerFactory.getLogger(ConsistentHashReactiveLoadBalancer.class);

    private final ObjectProvider<ServiceInstanceListSupplier> supplierProvider;
    private final String serviceId;
    private final LoadBalancePolicyRepository policyRepository;
    private final RoundRobinLoadBalancer delegate;
    private final HashKeyExtractor keyExtractor;
    private final HashKeyMissingLogger missingLogger;
    private final Cache<String, ConsistentHashRing> ringCache;

    public ConsistentHashReactiveLoadBalancer(ObjectProvider<ServiceInstanceListSupplier> supplierProvider,
                                              String serviceId,
                                              LoadBalancePolicyRepository policyRepository) {
        this(supplierProvider, serviceId, policyRepository,
                new RoundRobinLoadBalancer(supplierProvider, serviceId),
                new DefaultHashKeyExtractor(),
                new HashKeyMissingLogger(),
                Caffeine.newBuilder().<String, ConsistentHashRing>maximumSize(2).build());
    }

    /** 测试入口：注入可控的 delegate / keyExtractor / missingLogger / ringCache。 */
    ConsistentHashReactiveLoadBalancer(ObjectProvider<ServiceInstanceListSupplier> supplierProvider,
                                       String serviceId,
                                       LoadBalancePolicyRepository policyRepository,
                                       RoundRobinLoadBalancer delegate,
                                       HashKeyExtractor keyExtractor,
                                       HashKeyMissingLogger missingLogger,
                                       Cache<String, ConsistentHashRing> ringCache) {
        this.supplierProvider = supplierProvider;
        this.serviceId = serviceId;
        this.policyRepository = policyRepository;
        this.delegate = delegate;
        this.keyExtractor = keyExtractor;
        this.missingLogger = missingLogger;
        this.ringCache = ringCache;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Mono<Response<ServiceInstance>> choose(Request request) {
        // fail-open 承诺覆盖整条链路，不只是预期到的 key 缺失/supplier 缺失两个分支——
        // ringCache 的 loader（ConsistentHashRing.build）理论上可能因异常配置（如 I2 描述的
        // 虚拟节点数过大）抛出异常，这里兜底降级为轮询，避免请求以 5xx 结束
        return Mono.defer(() -> chooseInternal(request))
                .onErrorResume(e -> {
                    log.warn("Consistent hash routing failed, degraded to round-robin. serviceId={}", serviceId, e);
                    return delegate.choose(request);
                });
    }

    private Mono<Response<ServiceInstance>> chooseInternal(Request request) {
        LoadBalancePolicy policy = policyRepository.findByServiceId(serviceId).orElse(null);
        if (policy == null) {
            return delegate.choose(request);
        }
        // key 提取完全不依赖实例列表，必须放在 supplier.get() 之前：否则 key 缺失这条降级路径
        // 会先触发一次 supplier.get()（Nacos 查询），delegate.choose() 内部又会自己再查一次，
        // 把配置错误场景（通常是高频路径）的 Nacos 查询量和 boundedElastic 占用直接翻倍
        Optional<String> key = keyExtractor.extract(request, policy);
        if (key.isEmpty()) {
            missingLogger.warnIfDue(serviceId, policy);
            return delegate.choose(request);
        }
        ServiceInstanceListSupplier supplier = supplierProvider.getIfAvailable();
        if (supplier == null) {
            // 理论上不会发生：同一 serviceId 的 LB 子容器总会注册一个 Supplier bean；
            // 仍然按 fail-open 惯例兜底，避免防御性判断反而制造一个未覆盖的 NPE 分支
            return delegate.choose(request);
        }
        return supplier.get(request).next().flatMap(instances -> {
            String cacheKey = buildCacheKey(instances, policy);
            ConsistentHashRing ring = ringCache.get(cacheKey,
                    k -> ConsistentHashRing.build(instances, resolveVirtualNodes(policy)));
            return Mono.just(ring.route(key.get())
                    .<Response<ServiceInstance>>map(DefaultResponse::new)
                    .orElseGet(EmptyResponse::new));
        });
    }

    private static int resolveVirtualNodes(LoadBalancePolicy policy) {
        Integer configured = policy.virtualNodesPerWeight();
        return configured != null ? configured : LoadBalancePolicy.DEFAULT_VIRTUAL_NODES_PER_WEIGHT;
    }

    // 用精确字符串（排序后的 "host:port:weight" 拼接串 + "|vn=" + 虚拟节点数）做缓存 key，
    // 不做哈希摘要——不相等就是不相等，没有碰撞概率问题；virtualNodesPerWeight 参与 key
    // 计算，保证该参数单独变化时也能触发重建。
    private static String buildCacheKey(List<ServiceInstance> instances, LoadBalancePolicy policy) {
        String instancesPart = instances.stream()
                .map(instance -> instance.getHost() + ":" + instance.getPort() + ":" + ConsistentHashRing.resolveWeight(instance))
                .sorted()
                .collect(Collectors.joining(","));
        return instancesPart + "|vn=" + resolveVirtualNodes(policy);
    }
}

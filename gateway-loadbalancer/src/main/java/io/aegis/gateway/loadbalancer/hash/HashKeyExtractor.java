package io.aegis.gateway.loadbalancer.hash;

import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicy;
import org.springframework.cloud.client.loadbalancer.Request;

import java.util.Optional;

/**
 * 从一次负载均衡请求中按 policy 配置的来源提取一致性哈希路由 key。
 * <p>
 * 取不到值（header 未携带、request 上下文类型不符等）一律返回空，不抛异常——调用方
 * （{@code ConsistentHashReactiveLoadBalancer}）据此触发降级为轮询。
 */
public interface HashKeyExtractor {
    Optional<String> extract(Request<?> request, LoadBalancePolicy policy);
}

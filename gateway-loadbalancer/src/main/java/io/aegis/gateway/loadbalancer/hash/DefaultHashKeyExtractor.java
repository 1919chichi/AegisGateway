package io.aegis.gateway.loadbalancer.hash;

import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicy;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestData;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.http.HttpHeaders;

import java.util.Optional;

/**
 * {@link HashKeyExtractor} 的唯一实现，内部按 {@link LoadBalancePolicy#keySource()} 分支，
 * 而不是为每种来源写一个策略类——第一版只有两种来源，分支比多态更直接。
 * <p>
 * Request 上下文解包方式与 {@code NamespaceAwareNacosServiceInstanceListSupplier.extractAttributes()}
 * 同款：只有当 {@code request.getContext()} 是 {@link RequestDataContext} 且内部
 * {@link RequestData} 非空时才能取到请求头，否则视为缺失，跨模块保持一致的请求上下文访问约定。
 * <p>
 * CLIENT_IP 来源只能读 {@code X-Forwarded-For} 请求头：当前依赖的
 * {@code spring-cloud-loadbalancer} 版本里，{@link RequestData} 不携带原始连接的远端地址
 * （没有 {@code getRemoteAddress()} 这类方法），只保留了 method/URI/headers/cookies/attributes，
 * 因此无法回退到"真实 socket 远端地址"，网关前没有反向代理写入该头时会直接判定 key 缺失。
 */
public final class DefaultHashKeyExtractor implements HashKeyExtractor {

    private static final String CLIENT_IP_HEADER = "X-Forwarded-For";

    @Override
    public Optional<String> extract(Request<?> request, LoadBalancePolicy policy) {
        HttpHeaders headers = extractHeaders(request);
        if (headers == null) {
            return Optional.empty();
        }
        return switch (policy.keySource()) {
            case CLIENT_IP -> extractClientIp(headers);
            case HEADER -> extractHeaderValue(headers, policy.keyName());
        };
    }

    private Optional<String> extractClientIp(HttpHeaders headers) {
        String forwarded = headers.getFirst(CLIENT_IP_HEADER);
        if (forwarded == null || forwarded.isBlank()) {
            return Optional.empty();
        }
        // X-Forwarded-For 可能是多级代理拼接的逗号分隔列表，第一个是最原始的客户端地址
        String first = forwarded.split(",")[0].trim();
        return first.isEmpty() ? Optional.empty() : Optional.of(first);
    }

    private Optional<String> extractHeaderValue(HttpHeaders headers, String headerName) {
        String value = headers.getFirst(headerName);
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
    }

    private HttpHeaders extractHeaders(Request<?> request) {
        if (request == null || !(request.getContext() instanceof RequestDataContext context)) {
            return null;
        }
        RequestData requestData = context.getClientRequest();
        return requestData == null ? null : requestData.getHeaders();
    }
}

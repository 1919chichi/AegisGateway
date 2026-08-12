package io.aegis.gateway.loadbalancer.hash;

import io.aegis.gateway.loadbalancer.loadbalance.HashKeySource;
import io.aegis.gateway.loadbalancer.loadbalance.LoadBalancePolicy;
import io.aegis.gateway.loadbalancer.loadbalance.LoadBalanceStrategy;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.loadbalancer.DefaultRequest;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestData;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultHashKeyExtractorTest {

    private final DefaultHashKeyExtractor extractor = new DefaultHashKeyExtractor();

    @Test
    void extract_shouldReturnFirstForwardedIp_whenKeySourceIsClientIp() {
        LoadBalancePolicy policy = policy(HashKeySource.CLIENT_IP, null);
        Request<RequestDataContext> request = requestWithHeader("X-Forwarded-For", "1.2.3.4, 5.6.7.8");

        Optional<String> key = extractor.extract(request, policy);

        assertThat(key).contains("1.2.3.4");
    }

    @Test
    void extract_shouldReturnEmpty_whenClientIpHeaderMissing() {
        LoadBalancePolicy policy = policy(HashKeySource.CLIENT_IP, null);
        Request<RequestDataContext> request = requestWithHeader("X-Other-Header", "irrelevant");

        assertThat(extractor.extract(request, policy)).isEmpty();
    }

    @Test
    void extract_shouldReturnHeaderValue_whenKeySourceIsHeader() {
        LoadBalancePolicy policy = policy(HashKeySource.HEADER, "X-User-Id");
        Request<RequestDataContext> request = requestWithHeader("X-User-Id", "u10086");

        Optional<String> key = extractor.extract(request, policy);

        assertThat(key).contains("u10086");
    }

    @Test
    void extract_shouldReturnEmpty_whenConfiguredHeaderMissing() {
        LoadBalancePolicy policy = policy(HashKeySource.HEADER, "X-User-Id");
        Request<RequestDataContext> request = requestWithHeader("X-Other-Header", "irrelevant");

        assertThat(extractor.extract(request, policy)).isEmpty();
    }

    @Test
    void extract_shouldReturnEmpty_whenRequestContextIsNotRequestDataContext() {
        LoadBalancePolicy policy = policy(HashKeySource.HEADER, "X-User-Id");
        Request<String> request = new DefaultRequest<>("not-a-request-data-context");

        assertThat(extractor.extract(request, policy)).isEmpty();
    }

    private static LoadBalancePolicy policy(HashKeySource keySource, String keyName) {
        return new LoadBalancePolicy("order-service", LoadBalanceStrategy.CONSISTENT_HASH, keySource, keyName, null);
    }

    private static Request<RequestDataContext> requestWithHeader(String name, String value) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/orders").header(name, value).build());
        RequestData requestData = new RequestData(exchange.getRequest(), exchange.getAttributes());
        return new DefaultRequest<>(new RequestDataContext(requestData, "default"));
    }
}

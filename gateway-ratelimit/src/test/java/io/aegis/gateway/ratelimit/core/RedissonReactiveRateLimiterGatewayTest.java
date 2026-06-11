package io.aegis.gateway.ratelimit.core;

import io.aegis.gateway.ratelimit.model.RateLimitRule;
import io.aegis.gateway.ratelimit.model.RateLimitType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.redisson.api.RScript;
import org.redisson.api.RScriptReactive;
import org.redisson.api.RedissonReactiveClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RedissonReactiveRateLimiterGatewayTest {

    private final RedissonClientManager clientManager = mock(RedissonClientManager.class);

    @Test
    void shouldEvaluateTokenBucketScriptWithCapacityAndRefillRate() {
        RScriptReactive script = givenScript();
        when(script.<Boolean>eval(any(RScript.Mode.class), anyString(), any(RScript.ReturnType.class),
                anyList(), any(Object[].class))).thenReturn(Mono.just(true));
        RedissonReactiveRateLimiterGateway gateway = new RedissonReactiveRateLimiterGateway(clientManager);

        StepVerifier.create(gateway.tryAcquire("key", rule("r1", 10, 5))).expectNext(true).verifyComplete();

        ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(script).eval(
                eqMode(),
                any(String.class),
                eqReturnType(),
                eqKeys(List.of("key")),
                argsCaptor.capture());
        assertThat(argsCaptor.getValue()).containsExactly(10L, 5L, 1L, 60_000L);
    }

    @Test
    void shouldReturnFalseWhenScriptDeniesToken() {
        RScriptReactive script = givenScript();
        when(script.<Boolean>eval(any(RScript.Mode.class), anyString(), any(RScript.ReturnType.class),
                anyList(), any(Object[].class))).thenReturn(Mono.just(false));
        RedissonReactiveRateLimiterGateway gateway = new RedissonReactiveRateLimiterGateway(clientManager);

        StepVerifier.create(gateway.tryAcquire("key", rule("r1", 10, 5))).expectNext(false).verifyComplete();
    }

    @Test
    void shouldPropagateRedisErrorsForFilterFailOpenHandling() {
        RScriptReactive script = givenScript();
        when(script.<Boolean>eval(any(RScript.Mode.class), anyString(), any(RScript.ReturnType.class),
                anyList(), any(Object[].class)))
                .thenReturn(Mono.error(new IllegalStateException("redis unavailable")));
        RedissonReactiveRateLimiterGateway gateway = new RedissonReactiveRateLimiterGateway(clientManager);

        StepVerifier.create(gateway.tryAcquire("key", rule("r1", 10, 5)))
                .expectErrorMessage("redis unavailable")
                .verify();
    }

    @Test
    void shouldCompleteEmptyAndScheduleRetryWhenClientIsAbsent() {
        when(clientManager.current()).thenReturn(null);
        RedissonReactiveRateLimiterGateway gateway = new RedissonReactiveRateLimiterGateway(clientManager);

        // 空 Mono = 无决策，由 Filter fail-open；同时应触发带冷却的异步重建
        StepVerifier.create(gateway.tryAcquire("key", rule("r1", 10, 5))).verifyComplete();

        verify(clientManager).retryLater();
    }

    private RScriptReactive givenScript() {
        RedissonReactiveClient redisson = mock(RedissonReactiveClient.class);
        RScriptReactive script = mock(RScriptReactive.class);
        when(clientManager.current()).thenReturn(redisson);
        when(redisson.getScript()).thenReturn(script);
        return script;
    }

    private static RateLimitRule rule(String id, long capacity, long refillRate) {
        return new RateLimitRule(id, RateLimitType.SERVICE, capacity, refillRate, null, null);
    }

    private static RScript.Mode eqMode() {
        return org.mockito.ArgumentMatchers.eq(RScript.Mode.READ_WRITE);
    }

    private static RScript.ReturnType eqReturnType() {
        return org.mockito.ArgumentMatchers.eq(RScript.ReturnType.BOOLEAN);
    }

    private static List<Object> eqKeys(List<Object> keys) {
        return org.mockito.ArgumentMatchers.eq(keys);
    }
}

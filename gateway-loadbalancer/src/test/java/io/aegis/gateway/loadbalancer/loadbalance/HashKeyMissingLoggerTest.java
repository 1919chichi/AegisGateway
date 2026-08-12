package io.aegis.gateway.loadbalancer.loadbalance;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class HashKeyMissingLoggerTest {

    private final LoadBalancePolicy policy =
            new LoadBalancePolicy("order-service", LoadBalanceStrategy.CONSISTENT_HASH, HashKeySource.HEADER, "X-User-Id", null);

    @Test
    void warnIfDue_shouldLog_onFirstCall() {
        AtomicLong clock = new AtomicLong(0);
        HashKeyMissingLogger logger = new HashKeyMissingLogger(1000, clock::get);

        assertThat(logger.warnIfDue("order-service", policy)).isTrue();
    }

    @Test
    void warnIfDue_shouldNotLog_whenCalledAgainWithinWindow() {
        AtomicLong clock = new AtomicLong(0);
        HashKeyMissingLogger logger = new HashKeyMissingLogger(1000, clock::get);
        logger.warnIfDue("order-service", policy);

        clock.set(500); // 未超过 1000ms 窗口

        assertThat(logger.warnIfDue("order-service", policy)).isFalse();
    }

    @Test
    void warnIfDue_shouldLogAgain_afterWindowElapses() {
        AtomicLong clock = new AtomicLong(0);
        HashKeyMissingLogger logger = new HashKeyMissingLogger(1000, clock::get);
        logger.warnIfDue("order-service", policy);

        clock.set(1000); // 恰好到达窗口边界

        assertThat(logger.warnIfDue("order-service", policy)).isTrue();
    }

    @Test
    void warnIfDue_shouldThrottleIndependently_perServiceId() {
        AtomicLong clock = new AtomicLong(0);
        HashKeyMissingLogger logger = new HashKeyMissingLogger(1000, clock::get);
        logger.warnIfDue("order-service", policy);

        // 不同 serviceId 的节流状态互不影响，即使 order-service 刚打印过
        assertThat(logger.warnIfDue("user-service", policy)).isTrue();
    }
}

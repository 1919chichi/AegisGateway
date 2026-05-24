package io.aegis.gateway.gray.model;

import io.aegis.gateway.gray.matcher.HeaderGrayMatcher;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GrayRuleTest {

    @Test
    void toMatcher_shouldReturnHeaderGrayMatcherForHeaderType() {
        GrayRule rule = new GrayRule("header", "X-User-Type", "beta", "user-service-canary");
        assertThat(rule.toMatcher()).isInstanceOf(HeaderGrayMatcher.class);
    }

    @Test
    void toMatcher_shouldThrowIllegalArgumentExceptionForUnknownType() {
        GrayRule rule = new GrayRule("jwt", "userGroup", "beta", "user-service-canary");
        assertThatThrownBy(rule::toMatcher)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown gray rule type: jwt");
    }
}

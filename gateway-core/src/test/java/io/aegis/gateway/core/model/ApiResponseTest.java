package io.aegis.gateway.core.model;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class ApiResponseTest {

    @Test
    void success_shouldReturnCode200WithData() {
        ApiResponse<String> response = ApiResponse.success("hello");

        assertThat(response.code()).isEqualTo(200);
        assertThat(response.message()).isEqualTo("success");
        assertThat(response.data()).isEqualTo("hello");
        assertThat(response.timestamp()).isPositive();
    }

    @Test
    void error_withErrorCode_shouldReturnNullData() {
        ApiResponse<Void> response = ApiResponse.error(AegisErrorCode.ROUTE_NOT_FOUND);

        assertThat(response.code()).isEqualTo(40401);
        assertThat(response.message()).isEqualTo("Route not found");
        assertThat(response.data()).isNull();
        assertThat(response.timestamp()).isPositive();
    }

    @Test
    void error_withRateLimitPath_shouldReturn42901() {
        ApiResponse<Void> response = ApiResponse.error(AegisErrorCode.RATE_LIMIT_PATH);

        assertThat(response.code()).isEqualTo(42901);
    }

    @Test
    void error_withCircuitBreakerOpen_shouldReturn50300() {
        ApiResponse<Void> response = ApiResponse.error(AegisErrorCode.SERVICE_UNAVAILABLE);

        assertThat(response.code()).isEqualTo(50300);
    }
}

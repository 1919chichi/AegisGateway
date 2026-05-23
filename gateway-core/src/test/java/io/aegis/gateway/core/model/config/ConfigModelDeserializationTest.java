package io.aegis.gateway.core.model.config;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import static org.assertj.core.api.Assertions.assertThat;

class ConfigModelDeserializationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void shouldDeserializeAegisRoutesConfigWithOrder() throws Exception {
        String json = """
            {
              "routes": [
                {
                  "id": "user-service",
                  "uri": "lb://user-service",
                  "predicates": ["Path=/api/user/**"],
                  "filters": ["StripPrefix=1"],
                  "order": 1,
                  "metadata": {
                    "rateLimit": {"ruleId": "user-service-limit"},
                    "circuitBreaker": {"ruleId": "user-service-cb"},
                    "retry": {"maxAttempts": 3}
                  }
                }
              ]
            }
            """;

        AegisRoutesConfig config = objectMapper.readValue(json, AegisRoutesConfig.class);

        assertThat(config.routes()).hasSize(1);
        AegisRoute route = config.routes().get(0);
        assertThat(route.id()).isEqualTo("user-service");
        assertThat(route.uri()).isEqualTo("lb://user-service");
        assertThat(route.predicates()).containsExactly("Path=/api/user/**");
        assertThat(route.filters()).containsExactly("StripPrefix=1");
        assertThat(route.order()).isEqualTo(1);
        assertThat(route.metadata()).containsKey("rateLimit");
        assertThat(route.metadata()).containsKey("circuitBreaker");
    }

    @Test
    void shouldDeserializeGlobalConfig() throws Exception {
        String json = """
            {
              "cors": {
                "allowedOrigins": ["*"],
                "allowedMethods": ["GET", "POST"]
              },
              "auth": {
                "jwtSecret": "my-secret",
                "excludePaths": ["/api/public/**"]
              },
              "admin": {
                "apiKey": "admin-key-123"
              }
            }
            """;

        GlobalConfig config = objectMapper.readValue(json, GlobalConfig.class);

        assertThat(config.cors().allowedOrigins()).containsExactly("*");
        assertThat(config.auth().jwtSecret()).isEqualTo("my-secret");
        assertThat(config.admin().apiKey()).isEqualTo("admin-key-123");
    }
}

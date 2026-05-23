package io.aegis.gateway.core.route;

import io.aegis.gateway.core.model.config.AegisRoute;
import io.aegis.gateway.core.model.config.AegisRoutesConfig;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.context.ApplicationEventPublisher;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AegisRouteDefinitionRepositoryTest {

    @Mock
    private NacosConfigSyncService syncService;
    @Mock
    private ApplicationEventPublisher publisher;

    private AegisRouteDefinitionRepository repository;

    @BeforeEach
    void setUp() {
        when(syncService.getRoutesConfig()).thenReturn(AegisRoutesConfig.empty());
        repository = new AegisRouteDefinitionRepository(syncService, publisher);
    }

    @Test
    void getRouteDefinitions_shouldReturnEmptyWhenNoRoutes() {
        StepVerifier.create(repository.getRouteDefinitions())
            .verifyComplete();
    }

    @Test
    void getRouteDefinitions_shouldConvertAegisRouteToRouteDefinition() {
        AegisRoute route = new AegisRoute(
            "user-service",
            "lb://user-service",
            List.of("Path=/api/user/**"),
            List.of("StripPrefix=1"),
            1,
            Map.of()
        );
        when(syncService.getRoutesConfig())
            .thenReturn(new AegisRoutesConfig(List.of(route)));
        repository = new AegisRouteDefinitionRepository(syncService, publisher);

        StepVerifier.create(repository.getRouteDefinitions())
            .assertNext(def -> {
                assertThat(def.getId()).isEqualTo("user-service");
                assertThat(def.getUri().toString()).isEqualTo("lb://user-service");
                assertThat(def.getPredicates()).hasSize(1);
                assertThat(def.getFilters()).hasSize(1);
                assertThat(def.getOrder()).isEqualTo(1);
            })
            .verifyComplete();
    }

    @Test
    void onRoutesChange_shouldAtomicallyReplaceRoutesAndPublishEvent() {
        ArgumentCaptor<Consumer<AegisRoutesConfig>> listenerCaptor =
            ArgumentCaptor.forClass(Consumer.class);
        verify(syncService).registerRoutesListener(listenerCaptor.capture());

        AegisRoute newRoute = new AegisRoute(
            "order-service", "lb://order-service",
            List.of("Path=/api/order/**"), List.of(), 0, Map.of()
        );
        listenerCaptor.getValue().accept(new AegisRoutesConfig(List.of(newRoute)));

        verify(publisher).publishEvent(any());

        StepVerifier.create(repository.getRouteDefinitions())
            .assertNext(def -> assertThat(def.getId()).isEqualTo("order-service"))
            .verifyComplete();
    }

    @Test
    void save_shouldReturnError_becauseNacosIsTheSingleSourceOfTruth() {
        StepVerifier.create(repository.save(reactor.core.publisher.Mono.just(new RouteDefinition())))
            .expectError(UnsupportedOperationException.class)
            .verify();
    }

    @Test
    void delete_shouldReturnError_becauseNacosIsTheSingleSourceOfTruth() {
        StepVerifier.create(repository.delete(reactor.core.publisher.Mono.just("some-id")))
            .expectError(UnsupportedOperationException.class)
            .verify();
    }
}

package io.aegis.gateway.core.route;

import io.aegis.gateway.core.model.config.AegisRoute;
import io.aegis.gateway.core.model.config.AegisRoutesConfig;
import io.aegis.gateway.core.nacos.NacosConfigSyncService;
import org.springframework.cloud.gateway.event.RefreshRoutesEvent;
import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.cloud.gateway.route.RouteDefinitionRepository;
import org.springframework.context.ApplicationEventPublisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * 以 Nacos 为唯一数据源的 Spring Cloud Gateway RouteDefinitionRepository 实现。
 * <p>
 * 启动时从 {@link NacosConfigSyncService} 加载初始路由，并订阅后续变更；
 * 每次收到新配置时原子替换内存路由 Map，再发布 {@link RefreshRoutesEvent} 触发 SCG 重载。
 * <p>
 * {@link #save} 和 {@link #delete} 故意抛出 {@link UnsupportedOperationException}——
 * 所有路由变更必须经由 Admin API 写入 Nacos，不允许绕过 Nacos 直接修改内存状态。
 */
public class AegisRouteDefinitionRepository implements RouteDefinitionRepository {

    private final AtomicReference<Map<String, RouteDefinition>> routeStore =
            new AtomicReference<>(Map.of());
    private final ApplicationEventPublisher publisher;

    public AegisRouteDefinitionRepository(NacosConfigSyncService syncService,
                                          ApplicationEventPublisher publisher) {
        this.publisher = publisher;
        applyRoutes(syncService.getRoutesConfig().routes());
        syncService.registerRoutesListener(config -> {
            applyRoutes(config.routes());
            publisher.publishEvent(new RefreshRoutesEvent(this));
        });
    }

    private void applyRoutes(List<AegisRoute> routes) {
        Map<String, RouteDefinition> newStore = routes.stream()
            .collect(Collectors.toUnmodifiableMap(AegisRoute::id, this::toRouteDefinition));
        routeStore.set(newStore);
    }

    private RouteDefinition toRouteDefinition(AegisRoute route) {
        RouteDefinition def = new RouteDefinition();
        def.setId(route.id());
        def.setUri(URI.create(route.uri()));
        def.setOrder(route.order());
        def.setPredicates(route.predicates().stream()
            .map(PredicateDefinition::new)
            .toList());
        def.setFilters(route.filters().stream()
            .map(FilterDefinition::new)
            .toList());
        def.setMetadata(route.metadata());
        return def;
    }

    @Override
    public Flux<RouteDefinition> getRouteDefinitions() {
        return Flux.fromIterable(routeStore.get().values());
    }

    @Override
    public Mono<Void> save(Mono<RouteDefinition> route) {
        return Mono.error(new UnsupportedOperationException(
            "Route modification must go through Admin API → Nacos Config. Direct save is not supported."));
    }

    @Override
    public Mono<Void> delete(Mono<String> routeId) {
        return Mono.error(new UnsupportedOperationException(
            "Route deletion must go through Admin API → Nacos Config. Direct delete is not supported."));
    }
}

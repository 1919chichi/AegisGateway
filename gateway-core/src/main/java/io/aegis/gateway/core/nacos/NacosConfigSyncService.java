package io.aegis.gateway.core.nacos;

import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.AbstractListener;
import io.aegis.gateway.core.model.config.AegisRoutesConfig;
import io.aegis.gateway.core.model.config.GlobalConfig;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import tools.jackson.databind.ObjectMapper;

/**
 * 负责从 Nacos 拉取并订阅三个核心配置文件（路由、治理、全局），是所有配置的单一入口。
 * <p>
 * 启动时使用 Java 25 Structured Concurrency 并行加载三个配置；任意一个超时或失败
 * 会导致整体启动失败，防止网关以不完整配置上线。
 * <p>
 * 每个 Data ID 持有独立的单线程虚拟线程 Executor，保证同一配置的 Nacos 推送事件
 * 按到达顺序串行处理，避免乱序覆盖。
 * <p>
 * 其他模块通过 {@link #registerRoutesListener}、{@link #registerGovernanceListener}、
 * {@link #registerGlobalListener} 注册回调，即可在配置热更新时收到通知。
 */
public class NacosConfigSyncService {

    private static final Logger log = LoggerFactory.getLogger(NacosConfigSyncService.class);
    private static final long TIMEOUT_MS = 5000L;

    private final ConfigService configService;
    private final ObjectMapper objectMapper;
    private final String nacosGroup;

    private final AtomicReference<AegisRoutesConfig> routesConfig =
            new AtomicReference<>(AegisRoutesConfig.empty());
    private final AtomicReference<String> governanceConfigJson =
            new AtomicReference<>("{}");
    private final AtomicReference<GlobalConfig> globalConfig =
            new AtomicReference<>(GlobalConfig.defaults());

    private final List<Consumer<AegisRoutesConfig>> routesListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<String>> governanceListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<GlobalConfig>> globalListeners = new CopyOnWriteArrayList<>();

    // 每个 Data ID 独立的单线程虚拟线程 Executor，保证同一配置的更新串行有序
    private final ExecutorService routesExecutor =
            Executors.newSingleThreadExecutor(Thread.ofVirtual().name("nacos-routes-", 0).factory());
    private final ExecutorService governanceExecutor =
            Executors.newSingleThreadExecutor(Thread.ofVirtual().name("nacos-governance-", 0).factory());
    private final ExecutorService globalExecutor =
            Executors.newSingleThreadExecutor(Thread.ofVirtual().name("nacos-global-", 0).factory());

    public NacosConfigSyncService(ConfigService configService, ObjectMapper objectMapper, String nacosGroup) {
        this.configService = configService;
        this.objectMapper = objectMapper;
        this.nacosGroup = nacosGroup;
    }

    @PostConstruct
    public void init() throws Exception {
        // 使用 Structured Concurrency 并行加载三个 Nacos 配置，任一失败则整体失败
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.awaitAllSuccessfulOrThrow())) {
            scope.fork(() -> { loadAndSubscribeRoutes(); return null; });
            scope.fork(() -> { loadAndSubscribeGovernance(); return null; });
            scope.fork(() -> { loadAndSubscribeGlobal(); return null; });
            scope.join();
        }
    }

    private void loadAndSubscribeRoutes() throws Exception {
        String json = configService.getConfig(NacosConfigKeys.ROUTES, nacosGroup, TIMEOUT_MS);
        if (json != null) {
            updateRoutesConfig(json);
        }
        configService.addListener(NacosConfigKeys.ROUTES, nacosGroup, new AbstractListener() {
            @Override
            public void receiveConfigInfo(String configInfo) {
                routesExecutor.execute(() -> updateRoutesConfig(configInfo));
            }
        });
    }

    private void loadAndSubscribeGovernance() throws Exception {
        String json = configService.getConfig(NacosConfigKeys.GOVERNANCE, nacosGroup, TIMEOUT_MS);
        if (json != null) {
            updateGovernanceConfig(json);
        }
        configService.addListener(NacosConfigKeys.GOVERNANCE, nacosGroup, new AbstractListener() {
            @Override
            public void receiveConfigInfo(String configInfo) {
                governanceExecutor.execute(() -> updateGovernanceConfig(configInfo));
            }
        });
    }

    private void loadAndSubscribeGlobal() throws Exception {
        String json = configService.getConfig(NacosConfigKeys.GLOBAL, nacosGroup, TIMEOUT_MS);
        if (json != null) {
            updateGlobalConfig(json);
        }
        configService.addListener(NacosConfigKeys.GLOBAL, nacosGroup, new AbstractListener() {
            @Override
            public void receiveConfigInfo(String configInfo) {
                globalExecutor.execute(() -> updateGlobalConfig(configInfo));
            }
        });
    }

    private void updateRoutesConfig(String json) {
        try {
            AegisRoutesConfig config = objectMapper.readValue(json, AegisRoutesConfig.class);
            routesConfig.set(config);
            routesListeners.forEach(l -> l.accept(config));
        } catch (Exception e) {
            log.error("Failed to parse routes config", e);
        }
    }

    private void updateGovernanceConfig(String json) {
        try {
            // 至少校验为合法 JSON，防止模块拿到损坏数据
            objectMapper.readTree(json);
            governanceConfigJson.set(json);
            governanceListeners.forEach(l -> l.accept(json));
        } catch (Exception e) {
            log.error("Failed to parse governance config JSON", e);
        }
    }

    private void updateGlobalConfig(String json) {
        try {
            GlobalConfig config = objectMapper.readValue(json, GlobalConfig.class);
            globalConfig.set(config);
            globalListeners.forEach(l -> l.accept(config));
        } catch (Exception e) {
            log.error("Failed to parse global config", e);
        }
    }

    public AegisRoutesConfig getRoutesConfig() { return routesConfig.get(); }
    public String getGovernanceConfigJson() { return governanceConfigJson.get(); }
    public GlobalConfig getGlobalConfig() { return globalConfig.get(); }

    public void registerRoutesListener(Consumer<AegisRoutesConfig> listener) {
        routesListeners.add(listener);
    }

    public void registerGovernanceListener(Consumer<String> listener) {
        governanceListeners.add(listener);
    }

    public void registerGlobalListener(Consumer<GlobalConfig> listener) {
        globalListeners.add(listener);
    }

    @PreDestroy
    public void shutdown() {
        routesExecutor.shutdown();
        governanceExecutor.shutdown();
        globalExecutor.shutdown();
        try {
            if (!routesExecutor.awaitTermination(2, TimeUnit.SECONDS))     routesExecutor.shutdownNow();
            if (!governanceExecutor.awaitTermination(2, TimeUnit.SECONDS)) governanceExecutor.shutdownNow();
            if (!globalExecutor.awaitTermination(2, TimeUnit.SECONDS))     globalExecutor.shutdownNow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

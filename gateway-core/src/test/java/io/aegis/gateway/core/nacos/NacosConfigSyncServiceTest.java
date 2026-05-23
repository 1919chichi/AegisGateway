package io.aegis.gateway.core.nacos;

import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import io.aegis.gateway.core.model.config.AegisRoutesConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NacosConfigSyncServiceTest {

    @Mock
    private ConfigService configService;

    private NacosConfigSyncService syncService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        when(configService.getConfig(eq(NacosConfigKeys.ROUTES), eq(NacosConfigKeys.DEFAULT_GROUP), anyLong()))
            .thenReturn("{\"routes\":[]}");
        when(configService.getConfig(eq(NacosConfigKeys.GOVERNANCE), eq(NacosConfigKeys.DEFAULT_GROUP), anyLong()))
            .thenReturn("{}");
        when(configService.getConfig(eq(NacosConfigKeys.GLOBAL), eq(NacosConfigKeys.DEFAULT_GROUP), anyLong()))
            .thenReturn("{\"cors\":{\"allowedOrigins\":[\"*\"],\"allowedMethods\":[\"GET\"]},\"auth\":{\"jwtSecret\":\"\",\"excludePaths\":[]},\"admin\":{\"apiKey\":\"\"}}");

        syncService = new NacosConfigSyncService(configService, objectMapper, NacosConfigKeys.DEFAULT_GROUP);
        syncService.init();
    }

    @Test
    void init_shouldLoadInitialRoutesConfig() {
        AegisRoutesConfig routes = syncService.getRoutesConfig();
        assertThat(routes.routes()).isEmpty();
    }

    @Test
    void init_shouldSubscribeToNacosListeners() throws Exception {
        verify(configService, times(3)).addListener(any(), eq(NacosConfigKeys.DEFAULT_GROUP), any(Listener.class));
    }

    @Test
    void routeChange_shouldUpdateInMemoryConfigAndNotifyListener() throws Exception {
        ArgumentCaptor<Listener> listenerCaptor = ArgumentCaptor.forClass(Listener.class);
        verify(configService).addListener(eq(NacosConfigKeys.ROUTES), eq(NacosConfigKeys.DEFAULT_GROUP), listenerCaptor.capture());

        AtomicReference<AegisRoutesConfig> received = new AtomicReference<>();
        syncService.registerRoutesListener(received::set);

        String newRoutesJson = """
            {"routes":[{"id":"svc-a","uri":"lb://svc-a","predicates":["Path=/api/a/**"],"filters":[],"order":0,"metadata":{}}]}
            """;
        listenerCaptor.getValue().receiveConfigInfo(newRoutesJson);

        // 等待串行虚拟线程 Executor 处理完成
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(syncService.getRoutesConfig().routes()).hasSize(1);
            assertThat(syncService.getRoutesConfig().routes().get(0).id()).isEqualTo("svc-a");
            assertThat(received.get().routes()).hasSize(1);
        });
    }

    @Test
    void governanceChange_shouldNotifyRegisteredListener() throws Exception {
        ArgumentCaptor<Listener> listenerCaptor = ArgumentCaptor.forClass(Listener.class);
        verify(configService).addListener(eq(NacosConfigKeys.GOVERNANCE), eq(NacosConfigKeys.DEFAULT_GROUP), listenerCaptor.capture());

        AtomicReference<String> received = new AtomicReference<>();
        syncService.registerGovernanceListener(received::set);

        listenerCaptor.getValue().receiveConfigInfo("{\"rateLimits\":[]}");

        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() ->
            assertThat(received.get()).isEqualTo("{\"rateLimits\":[]}")
        );
    }
}

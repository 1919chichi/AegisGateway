# 构建与测试命令

```bash
# 构建可运行的 JAR
./gradlew :gateway-server:bootJar

# 运行所有测试
./gradlew test

# 运行单个模块的测试
./gradlew :gateway-core:test

# 运行单个测试类
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.route.AegisRouteDefinitionRepositoryTest"

# 运行单个测试方法
./gradlew :gateway-core:test --tests "io.aegis.gateway.core.route.AegisRouteDefinitionRepositoryTest.save_shouldReturnError_becauseNacosIsTheSingleSourceOfTruth"

# 启动网关（需要 Nacos 已运行）
java --enable-preview --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED -jar gateway-server/build/libs/gateway-server-*.jar
```

所有编译和测试任务都需要 `--enable-preview`（Java 25 预览特性），根 `build.gradle` 已统一配置，不要移除。

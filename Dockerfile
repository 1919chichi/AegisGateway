FROM eclipse-temurin:25-jre-alpine

WORKDIR /app

COPY gateway-server/build/libs/gateway-server-*.jar app.jar

ENV NACOS_SERVER_ADDR=127.0.0.1:8848 \
    NACOS_NAMESPACE= \
    AEGIS_NACOS_GROUP=aegis

EXPOSE 8080

ENTRYPOINT ["java", "--enable-preview", "-jar", "app.jar"]

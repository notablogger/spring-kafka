# ─── Build Stage ─────────────────────────────────────────────
FROM gradle:8.14-jdk21 AS build
WORKDIR /app
COPY --chown=gradle:gradle . .
RUN gradle clean bootJar --no-daemon

# ─── Run Stage ───────────────────────────────────────────────
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /app/build/libs/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]

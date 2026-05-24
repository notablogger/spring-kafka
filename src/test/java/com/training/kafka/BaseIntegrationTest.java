package com.training.kafka;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for all integration tests.
 *
 * Spins up four real containers via Testcontainers:
 *   - PostgreSQL 16    → source-of-truth DB for writes
 *   - Kafka 7.6.1      → event broker
 *   - Schema Registry  → Avro schema validation (real container, not mock)
 *   - MongoDB 7        → event log, read model
 *
 * Spring Boot is started on a random port so tests never clash with a running app.
 * @DirtiesContext ensures the application context (and DB state) is reset between test classes.
 *
 * All subclasses inherit the containers and property overrides automatically.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@DirtiesContext
public abstract class BaseIntegrationTest {

    // ─── PostgreSQL ──────────────────────────────────────────────
    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16")
                    .withDatabaseName("nik_kafka_db_test")
                    .withUsername("nikuser")
                    .withPassword("nikpassword");

    // ─── Kafka ───────────────────────────────────────────────────
    @Container
    static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    // ─── Schema Registry ─────────────────────────────────────────
    // Uses cp-schema-registry pointed at the Kafka container bootstrap servers.
    // Started after Kafka via withEnv — Testcontainers handles ordering.
    @Container
    static final GenericContainer<?> schemaRegistry =
            new GenericContainer<>("confluentinc/cp-schema-registry:7.6.1")
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                            "PLAINTEXT://" + kafka.getNetworkAliases().get(0) + ":9092")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .dependsOn(kafka);

    // ─── MongoDB ─────────────────────────────────────────────────
    @Container
    static final MongoDBContainer mongodb =
            new MongoDBContainer(DockerImageName.parse("mongo:7"));

    // ─── Property overrides ───────────────────────────────────────
    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        // Postgres
        registry.add("spring.datasource.url",             postgres::getJdbcUrl);
        registry.add("spring.datasource.username",         postgres::getUsername);
        registry.add("spring.datasource.password",         postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto",      () -> "create-drop");

        // Kafka
        registry.add("spring.kafka.bootstrap-servers",     kafka::getBootstrapServers);

        // Schema Registry — real container URL
        registry.add("spring.kafka.properties.schema.registry.url", () ->
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081));
        registry.add("spring.kafka.properties.specific.avro.reader", () -> "true");

        // MongoDB
        registry.add("spring.data.mongodb.uri", () ->
                "mongodb://" + mongodb.getHost() + ":" + mongodb.getMappedPort(27017) + "/nik_kafka_events_test");
    }
}

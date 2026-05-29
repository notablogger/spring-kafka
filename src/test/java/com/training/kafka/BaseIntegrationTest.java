package com.training.kafka;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for all integration tests.
 *
 * Uses a static initializer to start all containers once per JVM — this is the
 * recommended Testcontainers pattern for shared containers in abstract base classes.
 * The @Testcontainers + @Container approach on abstract classes is unreliable in CI
 * because @Container lifecycle is only managed for the class that declares @Testcontainers,
 * not its parent. The static initializer guarantees containers start before
 * @DynamicPropertySource runs, ensuring the mapped ports are available.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
public abstract class BaseIntegrationTest {

    static final Network network = Network.newNetwork();

    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16")
                    .withDatabaseName("kafka_training_db_test")
                    .withUsername("traininguser")
                    .withPassword("trainingpassword");

    static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                    .withNetwork(network)
                    .withNetworkAliases("kafka");

    static final GenericContainer<?> schemaRegistry =
            new GenericContainer<>("confluentinc/cp-schema-registry:7.6.1")
                    .withNetwork(network)
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
                    .dependsOn(kafka);

    static final MongoDBContainer mongodb =
            new MongoDBContainer(DockerImageName.parse("mongo:7"));

    static {
        // Start all containers before the Spring context is created.
        // Startables.deepStart respects dependsOn() — kafka starts before schemaRegistry.
        Startables.deepStart(postgres, kafka, schemaRegistry, mongodb).join();
    }

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        // Postgres
        registry.add("spring.datasource.url",             postgres::getJdbcUrl);
        registry.add("spring.datasource.username",         postgres::getUsername);
        registry.add("spring.datasource.password",         postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto",      () -> "create-drop");

        // Kafka — use the mapped bootstrap server (localhost:PORT)
        registry.add("spring.kafka.bootstrap-servers",     kafka::getBootstrapServers);

        // Schema Registry — mapped port from the test runner host
        registry.add("spring.kafka.properties.schema.registry.url", () ->
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081));
        registry.add("spring.kafka.properties.specific.avro.reader", () -> "true");

        // MongoDB — mapped port from the test runner host
        registry.add("spring.data.mongodb.uri", () ->
                "mongodb://" + mongodb.getHost() + ":" + mongodb.getMappedPort(27017) + "/kafka_training_events_test");
    }
}

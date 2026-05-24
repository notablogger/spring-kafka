# Build Reference — `build.gradle`

Everything in the build file explained — every plugin, every dependency, every configuration block, and why it's there.

---

## Overview

This project uses **Gradle** as its build tool with the **Groovy DSL** (`build.gradle`). Gradle reads this file to know how to compile the code, resolve dependencies, run tests, and produce the final JAR. The wrapper (`gradlew`) pins the Gradle version so every developer and CI environment uses exactly the same build toolchain without installing Gradle manually.

---

## Gradle Wrapper

```
gradle/wrapper/gradle-wrapper.properties
gradlew          ← Unix shell script
gradlew.bat      ← Windows batch script
```

| Command | What it does |
|---|---|
| `./gradlew build` | Compiles, runs tests, produces JAR in `build/libs/` |
| `./gradlew test` | Runs the test suite only |
| `./gradlew bootRun` | Starts the Spring Boot app locally without Docker |
| `./gradlew clean` | Deletes the `build/` directory |

The wrapper downloads the pinned Gradle version on first run — no global Gradle install needed.

---

## Plugins

```groovy
plugins {
    id 'java'
    id 'org.springframework.boot' version '4.0.6'
    id 'io.spring.dependency-management' version '1.1.7'
    id 'com.github.davidmc24.gradle.plugin.avro' version '1.9.1'
}
```

| Plugin | Version | Purpose |
|---|---|---|
| `java` | (built-in) | Core Gradle plugin — adds `compileJava`, `test`, `jar` tasks. Required as the base for all Java projects |
| `org.springframework.boot` | `4.0.6` | Adds `bootJar` task that produces a self-contained executable JAR (fat JAR) with an embedded Tomcat. Also adds `bootRun` to start the app from Gradle. Sets the Spring Boot BOM version so Spring dependency versions are managed automatically |
| `io.spring.dependency-management` | `1.1.7` | Works alongside the Spring Boot plugin — imports the Spring Boot BOM so you don't have to specify versions for Spring libraries (e.g. `spring-boot-starter-web` has no explicit version because this plugin resolves it) |
| `com.github.davidmc24.gradle.plugin.avro` | `1.9.1` | Watches `src/main/avro/*.avsc` files and generates Java classes into `build/generated-main-avro-java/` at compile time. Without this plugin, you'd have to run the Avro code generator manually |

---

## Project Coordinates

```groovy
group = 'com.training.kafka'
version = '0.0.1-SNAPSHOT'
```

| Setting | Value | Purpose |
|---|---|---|
| `group` | `com.training.kafka` | Maven group ID — identifies the organisation. Also becomes the root Java package name by convention |
| `version` | `0.0.1-SNAPSHOT` | Project version — `SNAPSHOT` means this is a development build, not a release |

---

## Java Toolchain

```groovy
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}
```

Declares that this project requires **Java 21**. Gradle's toolchain support automatically downloads and uses the correct JDK if it isn't present — no manual `JAVA_HOME` management needed. Java 21 is an LTS release and is required for Spring Boot 4.

---

## Repositories

```groovy
repositories {
    mavenCentral()
    maven { url 'https://packages.confluent.io/maven/' }
}
```

| Repository | Purpose |
|---|---|
| `mavenCentral()` | The standard public Maven repository — where almost all open-source Java libraries are published |
| `https://packages.confluent.io/maven/` | Confluent's private Maven repository — **required** for `kafka-avro-serializer` and `kafka-schema-registry-client`. These are not published to Maven Central, so the build fails to resolve them without this entry |

---

## Dependencies

### Spring Boot Starters

```groovy
implementation 'org.springframework.boot:spring-boot-starter-data-jpa'
implementation 'org.springframework.boot:spring-boot-starter-data-mongodb'
implementation 'org.springframework.boot:spring-boot-starter-kafka'
implementation 'org.springframework.boot:spring-boot-starter-web'
implementation 'org.springframework.boot:spring-boot-starter-validation'
```

Spring Boot starters are curated dependency bundles. Each one pulls in a set of related libraries and auto-configures them from `application.yml` — you get a working integration with zero manual wiring.

| Starter | What it pulls in | What it configures |
|---|---|---|
| `spring-boot-starter-data-jpa` | Hibernate ORM, Spring Data JPA, HikariCP connection pool | `EntityManagerFactory`, `JpaTransactionManager`, repository scanning |
| `spring-boot-starter-data-mongodb` | Spring Data MongoDB, MongoDB Java driver | `MongoClient`, `MongoTemplate`, repository scanning |
| `spring-boot-starter-kafka` | Spring Kafka, Apache Kafka client | `KafkaTemplate`, `KafkaListenerContainerFactory`, consumer/producer factories |
| `spring-boot-starter-web` | Spring MVC, embedded Tomcat, Jackson JSON | `DispatcherServlet`, JSON serialisation, `@RestController` support |
| `spring-boot-starter-validation` | Hibernate Validator, Jakarta Validation API | `@Valid` processing on controller method parameters |

> **Note:** `spring-boot-starter-web` and `spring-boot-starter-validation` must be declared separately. A common mistake is using `spring-boot-starter-webmvc` (which doesn't exist) or omitting `validation` (which silently disables `@NotBlank`, `@Email` etc.).

---

### Database Driver

```groovy
runtimeOnly 'org.postgresql:postgresql'
```

| Dependency | Scope | Purpose |
|---|---|---|
| `org.postgresql:postgresql` | `runtimeOnly` | The JDBC driver for PostgreSQL. `runtimeOnly` means it's on the classpath at runtime but not needed to compile the source code — Hibernate uses it via JDBC interfaces, not directly |

---

### Avro and Schema Registry

```groovy
implementation('io.confluent:kafka-avro-serializer:7.6.1') {
    exclude group: 'io.swagger.core.v3', module: 'swagger-annotations'
}
implementation('io.confluent:kafka-schema-registry-client:7.6.1') {
    exclude group: 'io.swagger.core.v3', module: 'swagger-annotations'
}
implementation 'org.apache.avro:avro:1.11.3'
```

| Dependency | Version | Purpose |
|---|---|---|
| `io.confluent:kafka-avro-serializer` | `7.6.1` | Provides `KafkaAvroSerializer` and `KafkaAvroDeserializer` — handles Avro binary encoding and the 5-byte Schema Registry wire format prefix (magic byte + schema ID) |
| `io.confluent:kafka-schema-registry-client` | `7.6.1` | HTTP client used internally by the serialisers to register schemas with and fetch schemas from the Schema Registry REST API |
| `org.apache.avro:avro` | `1.11.3` | Core Avro library — binary encoding engine, `SpecificRecord` base class, logical type converters (`DecimalConversion`, `DateConversion`) |

**Why the `exclude` blocks?**
Both Confluent artifacts transitively depend on `io.swagger.core.v3:swagger-annotations` at an older version that conflicts with SpringDoc 3.x (which this project uses for Swagger UI). Without the excludes, Gradle resolves the wrong version and the Swagger UI breaks or the build fails with classpath conflicts.

---

### Lombok

```groovy
compileOnly 'org.projectlombok:lombok'
annotationProcessor 'org.projectlombok:lombok'
```

| Dependency | Scope | Purpose |
|---|---|---|
| `org.projectlombok:lombok` | `compileOnly` | The Lombok annotation library — provides `@Getter`, `@Setter`, `@Builder`, `@RequiredArgsConstructor`, `@Slf4j` etc. `compileOnly` because Lombok is only needed at compile time; the generated bytecode has no Lombok dependency at runtime |
| `org.projectlombok:lombok` | `annotationProcessor` | The annotation processor that Gradle runs during `compileJava` to generate the actual getter/setter/constructor/builder code. Both entries are required — `compileOnly` for the annotations to be visible, `annotationProcessor` for the code generation to run |

---

### MapStruct

```groovy
implementation 'org.mapstruct:mapstruct:1.6.3'
annotationProcessor 'org.mapstruct:mapstruct-processor:1.6.3'
annotationProcessor 'org.projectlombok:lombok-mapstruct-binding:0.2.0'
```

| Dependency | Scope | Version | Purpose |
|---|---|---|---|
| `org.mapstruct:mapstruct` | `implementation` | `1.6.3` | The MapStruct annotation library — provides `@Mapper`, `@Mapping`, `@Context`. Needed at runtime because the generated implementation class references it |
| `org.mapstruct:mapstruct-processor` | `annotationProcessor` | `1.6.3` | The annotation processor that generates the mapper implementation class (e.g. `EmployeeToEventMapperImpl`) at compile time from the `@Mapper` interface |
| `org.projectlombok:lombok-mapstruct-binding` | `annotationProcessor` | `0.2.0` | **Critical ordering fix** — Lombok and MapStruct both run as annotation processors. Without this binding, Lombok runs after MapStruct, so MapStruct can't see the Lombok-generated getters/setters and generates broken mapper code. This artifact forces Lombok to run first |

---

### OpenAPI / Swagger

```groovy
implementation 'org.springdoc:springdoc-openapi-starter-webmvc-ui:3.0.0'
```

| Dependency | Version | Purpose |
|---|---|---|
| `org.springdoc:springdoc-openapi-starter-webmvc-ui` | `3.0.0` | Generates an OpenAPI 3 spec from your `@RestController` classes at runtime and serves Swagger UI at `/swagger-ui.html`. Also exposes the raw JSON spec at `/api-docs`. Version 3.x is required for Spring Boot 4 compatibility |

---

### Test Dependencies

```groovy
testImplementation 'org.springframework.boot:spring-boot-starter-test'
testImplementation platform('org.testcontainers:testcontainers-bom:1.20.4')
testImplementation 'org.testcontainers:testcontainers'
testImplementation 'org.testcontainers:postgresql'
testImplementation 'org.testcontainers:kafka'
testImplementation 'org.testcontainers:mongodb'
testImplementation 'org.testcontainers:junit-jupiter'
testImplementation 'org.awaitility:awaitility:4.2.2'
testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
```

| Dependency | Scope | Purpose |
|---|---|---|
| `spring-boot-starter-test` | `testImplementation` | Pulls in JUnit 5, Mockito, AssertJ, Spring Test (`@SpringBootTest`, `@DynamicPropertySource`) — the standard Spring testing toolkit |
| `testcontainers-bom` | `platform(...)` | The Testcontainers Bill of Materials — imported as a platform so all `org.testcontainers:*` modules use consistent, compatible versions without specifying each one individually |
| `org.testcontainers:testcontainers` | `testImplementation` | Core Testcontainers library — `@Container`, `GenericContainer`, lifecycle management, Docker socket communication |
| `org.testcontainers:postgresql` | `testImplementation` | Provides `PostgreSQLContainer` — a pre-configured container class for Postgres with `getJdbcUrl()`, `getUsername()`, `getPassword()` helpers |
| `org.testcontainers:kafka` | `testImplementation` | Provides `KafkaContainer` — a pre-configured Confluent Kafka container with `getBootstrapServers()` |
| `org.testcontainers:mongodb` | `testImplementation` | Provides `MongoDBContainer` — a pre-configured MongoDB container |
| `org.testcontainers:junit-jupiter` | `testImplementation` | JUnit 5 integration for Testcontainers — enables `@Testcontainers` and `@Container` annotations to manage container lifecycle automatically per test class |
| `org.awaitility:awaitility` | `testImplementation` | Provides `await().atMost(...).until(...)` — used to wait for the async Kafka consumer to process and save to MongoDB before asserting. Without this, tests would be flaky race conditions |
| `org.junit.platform:junit-platform-launcher` | `testRuntimeOnly` | Required by Gradle to discover and run JUnit 5 tests. `testRuntimeOnly` because it's only needed when actually running tests, not when compiling them |

---

## Avro Plugin Configuration

```groovy
avro {
    stringType = 'String'
    fieldVisibility = 'PRIVATE'
}

sourceSets.main.java.srcDirs += ["$buildDir/generated-main-avro-java"]
```

| Setting | Value | Purpose |
|---|---|---|
| `stringType = 'String'` | `String` | Tells the Avro plugin to generate `java.lang.String` fields instead of Avro's default `CharSequence`. Without this, every string field access requires `.toString()` — noisy and error-prone |
| `fieldVisibility = 'PRIVATE'` | `PRIVATE` | Generated fields are `private` with public getters/setters. Avro's default is `public` fields, which bypasses encapsulation |
| `sourceSets.main.java.srcDirs +=` | generated path | Adds the Avro-generated sources directory to the Java compile path — without this, `javac` doesn't know the generated `EmployeeEvent`, `DepartmentInfo`, `EventType` classes exist and compilation fails |

---

## Test Task Configuration

```groovy
tasks.named('test') {
    useJUnitPlatform()
}
```

Tells Gradle to use the **JUnit Platform** (JUnit 5's test engine) to discover and run tests. Without this, Gradle defaults to JUnit 4 and `@Test` annotations from JUnit 5 are silently ignored — tests appear to pass but never actually run.

---

## Dependency Scopes — Quick Reference

| Scope | Compile | Runtime | Test compile | Test runtime |
|---|---|---|---|---|
| `implementation` | ✅ | ✅ | ❌ | ❌ |
| `compileOnly` | ✅ | ❌ | ❌ | ❌ |
| `runtimeOnly` | ❌ | ✅ | ❌ | ❌ |
| `annotationProcessor` | processor only | ❌ | ❌ | ❌ |
| `testImplementation` | ❌ | ❌ | ✅ | ✅ |
| `testRuntimeOnly` | ❌ | ❌ | ❌ | ✅ |

---

## Full Dependency Tree (logical groups)

```
Spring Boot
├── spring-boot-starter-web          → REST layer (Spring MVC + Tomcat + Jackson)
├── spring-boot-starter-validation   → Bean validation (@NotBlank, @Email etc.)
├── spring-boot-starter-data-jpa     → Postgres write path (Hibernate + Spring Data)
├── spring-boot-starter-data-mongodb → MongoDB read path (Spring Data MongoDB)
└── spring-boot-starter-kafka        → Kafka producer + consumer (Spring Kafka)

Database
└── postgresql (runtime)             → JDBC driver for Postgres

Kafka / Avro
├── kafka-avro-serializer            → KafkaAvroSerializer / Deserializer
├── kafka-schema-registry-client     → Schema Registry HTTP client
└── avro                             → Core Avro binary encoding + logical types

Code generation (compile-time only)
├── lombok (compileOnly)             → @Getter, @Builder, @Slf4j etc.
├── lombok (annotationProcessor)     → generates the boilerplate
├── mapstruct                        → @Mapper interfaces
├── mapstruct-processor              → generates mapper implementations
└── lombok-mapstruct-binding         → ensures Lombok runs before MapStruct

API Documentation
└── springdoc-openapi-starter-webmvc-ui → Swagger UI + OpenAPI spec

Testing
├── spring-boot-starter-test         → JUnit 5, Mockito, AssertJ, Spring Test
├── testcontainers-bom               → version management for all TC modules
├── testcontainers:postgresql        → PostgreSQLContainer
├── testcontainers:kafka             → KafkaContainer
├── testcontainers:mongodb           → MongoDBContainer
├── testcontainers:junit-jupiter     → @Container lifecycle management
├── awaitility                       → async assertion polling
└── junit-platform-launcher          → Gradle test discovery
```


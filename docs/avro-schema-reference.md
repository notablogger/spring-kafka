# Avro Schema Reference — `message.avsc`

How the Avro schema is structured, why each field is defined the way it is, and how it flows through the system.

---

## What is Avro and Why Use It?

Avro is a **binary serialisation format** — it compresses messages before sending them over Kafka, unlike JSON which sends raw text. But the real reason to use Avro here is the **contract**: every producer and consumer must agree on the message shape, and that contract is enforced at runtime by the Schema Registry.

Without Avro:
- A producer can send `{ "salary": "75000.00" }` (string) and a consumer expecting a number silently breaks
- No centralised record of what the message looks like
- Schema changes are invisible to other services

With Avro + Schema Registry:
- The schema is registered once, versioned, and enforced
- Any incompatible change is rejected before a bad message reaches the topic
- Consumers can evolve independently using schema compatibility rules

---

## The Schema File

**Location:** `src/main/avro/message.avsc`

```json
{
  "type": "record",
  "name": "EmployeeEvent",
  "namespace": "com.training.kafka.avro",
  "fields": [
    { "name": "id",           "type": "long" },
    { "name": "firstName",    "type": "string" },
    { "name": "lastName",     "type": "string" },
    { "name": "email",        "type": "string" },
    { "name": "salary",       "type": { "type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2 } },
    { "name": "hireDate",     "type": { "type": "int",   "logicalType": "date" } },
    { "name": "department",   "type": {
        "type": "record",
        "name": "DepartmentInfo",
        "fields": [
          { "name": "id",       "type": "long" },
          { "name": "name",     "type": "string" },
          { "name": "location", "type": "string" }
        ]
      }
    },
    { "name": "eventType",      "type": { "type": "enum", "name": "EventType", "symbols": ["CREATED", "UPDATED", "DELETED"] } },
    { "name": "eventTimestamp", "type": "long", "logicalType": "timestamp-millis" }
  ]
}
```

---

## Field-by-Field Breakdown

| Field | Avro Type | Java Type (generated) | Why this choice |
|---|---|---|---|
| `id` | `long` | `long` | Employee's DB primary key — always a whole number |
| `firstName` | `string` | `String` | Plain text, no special encoding needed |
| `lastName` | `string` | `String` | Plain text |
| `email` | `string` | `String` | Plain text — validation happens at the API layer, not here |
| `salary` | `bytes` + logical `decimal` | `BigDecimal` | See below — floating point is not safe for money |
| `hireDate` | `int` + logical `date` | `LocalDate` | See below — compact date without time zone |
| `department` | nested `record` | `DepartmentInfo` | See below — snapshot of department at event time |
| `eventType` | `enum` | `EventType` | Enforces only valid values: CREATED, UPDATED, DELETED |
| `eventTimestamp` | `long` + logical `timestamp-millis` | `long` | Milliseconds since epoch — consumers can parse to `Instant` |

---

## The Three Interesting Choices

### 1. `salary` — Why `bytes` + `decimal`, not `float` or `double`?

```json
{ "type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2 }
```

`float` and `double` are **IEEE 754 floating-point** — they can't represent most decimal fractions exactly. For example:

```
0.1 + 0.2 = 0.30000000000000004  (in floating point)
```

For money, that's a bug. `decimal` with fixed `precision` and `scale` stores the value as an exact integer scaled by 10^-2 (i.e., 75000.00 is stored as the integer 7500000). No rounding errors.

- `precision: 18` → up to 18 significant digits total
- `scale: 2` → 2 digits after the decimal point (cents)
- Wire format: `bytes` (raw byte array of the unscaled integer)
- Java type after conversion: `BigDecimal` — the standard Java type for exact decimal arithmetic

The conversion is handled automatically by Avro's `DecimalConversion`, registered in the generated class:
```java
MODEL$.addLogicalTypeConversion(new org.apache.avro.Conversions.DecimalConversion());
```

---

### 2. `hireDate` — Why `int` + `date`, not `string`?

```json
{ "type": "int", "logicalType": "date" }
```

Storing dates as strings (`"2024-01-15"`) seems obvious but has real problems: timezone ambiguity, locale-dependent parsing, and no native ordering or comparison. Avro's `date` logical type stores the date as an **integer count of days since the Unix epoch (1970-01-01)**. This means:

- Compact — 4 bytes, not 10
- Unambiguous — no timezone involved, it's just a calendar date
- Sortable — dates sort correctly as integers
- Java type after conversion: `LocalDate` — date without time, exactly what a hire date is

The conversion is registered in the generated class:
```java
MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.DateConversion());
```

---

### 3. `department` — Why embed it as a nested record?

```json
{
  "type": "record",
  "name": "DepartmentInfo",
  "fields": [
    { "name": "id",       "type": "long" },
    { "name": "name",     "type": "string" },
    { "name": "location", "type": "string" }
  ]
}
```

The department is **denormalised into the event**. Rather than storing just a `departmentId` (which would require the consumer to look up the department separately), the full department snapshot — name and location at the time of the event — is embedded directly.

Why this matters:
- **Consumer is self-contained** — `EmployeeEventConsumer` saves the full document to MongoDB without any extra DB query
- **Historical accuracy** — if a department is renamed after the event, the event still reflects what the department was called at that time
- **No join at read time** — `GET /employees` returns department name/location from MongoDB directly, no Postgres lookup needed

This is the **event-carried state transfer** pattern — events carry all the state a consumer needs to act, not just IDs.

---

## Code Generation

The `.avsc` file is the **source of truth**. Java classes are generated from it at build time by the Avro Gradle plugin — you never write `EmployeeEvent.java` or `DepartmentInfo.java` by hand.

```
src/main/avro/message.avsc
        ↓  (./gradlew build)
build/generated-main-avro-java/com/training/kafka/avro/
    ├── EmployeeEvent.java    ← generated, do not edit
    ├── DepartmentInfo.java   ← generated, do not edit
    └── EventType.java        ← generated, do not edit
```

The generated classes implement `SpecificRecord` — they are strongly typed Java objects that Avro knows how to serialise and deserialise directly. This is why `specific.avro.reader: true` is set in `application.yml` — it tells the deserialiser to produce `EmployeeEvent` instances, not generic maps.

---

## Schema Registration

Before the producer can send its first message, the schema must be registered with the Schema Registry. This project does it automatically via the `schema-registry-init` Docker Compose service, which runs `scripts/register-schema.sh` on startup:

```bash
# Simplified version of what the script does
curl -X POST http://schema-registry:8081/subjects/EmployeeEvent-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{ "schema": "<escaped .avsc content>" }'
```

The subject name `EmployeeEvent-value` follows the **TopicNameStrategy** convention: `{topic-name}-value`. The Schema Registry stores this as version 1. On subsequent starts, the same schema is a no-op (already registered).

---

## Schema Evolution

Avro schemas can evolve over time without breaking existing consumers, as long as changes are **backward compatible**:

| Change | Compatible? | Why |
|---|---|---|
| Add a new field with a default value | ✅ Yes | Old consumers ignore unknown fields |
| Remove a field | ⚠️ Only with default | Consumers that expect it get the default |
| Rename a field | ❌ No | Treated as delete + add — breaking |
| Change a field type (e.g. `int` → `long`) | ❌ No | Binary encoding is different |

To add a field safely:
```json
{ "name": "phoneNumber", "type": ["null", "string"], "default": null }
```

The `["null", "string"]` union makes the field optional — existing messages without it will deserialise with `null`.

---

## Sample Message (JSON representation)

What an `EmployeeEvent` looks like in human-readable form (as shown in Confluent Control Center):

```json
{
  "id": 42,
  "firstName": "Jane",
  "lastName": "Smith",
  "email": "jane.smith@example.com",
  "salary": 85000.00,
  "hireDate": "2023-03-15",
  "department": {
    "id": 3,
    "name": "Engineering",
    "location": "London"
  },
  "eventType": "CREATED",
  "eventTimestamp": 1748044800000
}
```

On the wire, this is binary (Avro-encoded) with a 5-byte Schema Registry prefix (magic byte + schema ID) — typically ~60–80 bytes instead of ~200+ bytes as JSON.

---

## Spring Integration

How the Avro schema connects to Spring Boot — from `build.gradle` to producer to consumer.

### 1. Dependencies (`build.gradle`)

Three things are needed: the Avro library, the Confluent serialisers, and the Avro Gradle plugin to generate Java classes from the `.avsc` file.

```groovy
plugins {
    id 'com.github.davidmc24.gradle.plugin.avro' version '1.9.1'
}

repositories {
    mavenCentral()
    maven { url 'https://packages.confluent.io/maven/' }
}

dependencies {
    implementation 'org.apache.avro:avro:1.11.3'

    implementation('io.confluent:kafka-avro-serializer:7.6.1') {
        exclude group: 'io.swagger.core.v3', module: 'swagger-annotations'
    }
    implementation('io.confluent:kafka-schema-registry-client:7.6.1') {
        exclude group: 'io.swagger.core.v3', module: 'swagger-annotations'
    }
}

avro {
    stringType = 'String'
    fieldVisibility = 'PRIVATE'
}

sourceSets.main.java.srcDirs += ["$buildDir/generated-main-avro-java"]
```

| Library / Plugin | Purpose |
|---|---|
| `com.github.davidmc24.gradle.plugin.avro` | Gradle plugin — watches `src/main/avro/*.avsc` and generates Java classes into `build/generated-main-avro-java` at compile time |
| `org.apache.avro:avro` | Core Avro library — binary encoding/decoding, `SpecificRecord` base class, logical type conversions (`DecimalConversion`, `DateConversion`) |
| `io.confluent:kafka-avro-serializer` | Provides `KafkaAvroSerializer` and `KafkaAvroDeserializer` — prepends the 5-byte Schema Registry magic byte + schema ID before the Avro binary payload |
| `io.confluent:kafka-schema-registry-client` | HTTP client used internally by the serialisers to POST schemas to and GET schemas from the Schema Registry REST API |
| Confluent Maven repo | Confluent artifacts are not published to Maven Central — this repo must be declared or the build fails to resolve them |
| `stringType = 'String'` | Tells the plugin to generate `String` fields instead of `CharSequence` — avoids having to call `.toString()` everywhere |
| `fieldVisibility = 'PRIVATE'` | Generated fields are `private` with getters/setters — instead of Avro's default `public` fields |
| `exclude swagger-annotations` | `kafka-avro-serializer` transitively pulls in an older `swagger-annotations` that conflicts with SpringDoc 3.x. The `exclude` prevents the version clash that would otherwise break the build |

---

### 2. Generated Classes

| Class | Package | Purpose |
|---|---|---|
| `EmployeeEvent` | `com.training.kafka.avro` | Generated from `message.avsc` — implements `SpecificRecord`. Never edit directly; re-generated on every `./gradlew build` |
| `DepartmentInfo` | `com.training.kafka.avro` | Nested record — generated as a separate class even though it's defined inline in the schema |
| `EventType` | `com.training.kafka.avro` | Generated Java enum — values are `CREATED`, `UPDATED`, `DELETED` exactly as declared in the schema `symbols` array |
| `SpecificRecordBase` | `org.apache.avro.specific` | Base class all generated records extend — implements the `SpecificRecord` interface that Avro serialisers require |
| `DecimalConversion` | `org.apache.avro.Conversions` | Registered in the generated class's static initialiser — converts between Avro `bytes` wire format and Java `BigDecimal`. Without this, salary would be a raw `ByteBuffer` |
| `DateConversion` | `org.apache.avro.data.TimeConversions` | Registered in the generated class's static initialiser — converts between Avro `int` (days since epoch) and Java `LocalDate`. Without this, hireDate would be a raw `int` |

---

### 3. `application.yml` — Wiring Avro into Spring Kafka

```yaml
spring:
  kafka:
    bootstrap-servers: kafka:29092
    properties:
      schema.registry.url: http://schema-registry:8081
      specific.avro.reader: true
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
```

| Setting | Class / Value | Purpose |
|---|---|---|
| `schema.registry.url` | URL string | Shared by both producer and consumer — producer uses it to register the schema on first send, consumer uses it to fetch the schema by ID when deserialising |
| `specific.avro.reader: true` | `boolean` | Without this, `KafkaAvroDeserializer` returns a `GenericRecord` (a map-like object). With it, deserialisation produces a strongly typed `EmployeeEvent` instance |
| `KafkaAvroSerializer` | `io.confluent.kafka.serializers` | Serialiser for the message value — looks up or registers the schema, then encodes the `EmployeeEvent` as Avro binary with a 5-byte prefix |
| `KafkaAvroDeserializer` | `io.confluent.kafka.serializers` | Deserialiser — reads the 5-byte prefix, fetches the schema by ID from Schema Registry, then decodes bytes into `EmployeeEvent` |
| `StringSerializer` | `org.apache.kafka.common.serialization` | Serialises the message key (employee ID as string) to bytes |
| `StringDeserializer` | `org.apache.kafka.common.serialization` | Deserialises the message key bytes back to `String` |

---

### 4. Producer — `EmployeeEventProducer`

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class EmployeeEventProducer {

    private final KafkaTemplate<String, EmployeeEvent> kafkaTemplate;
    private final EmployeeToEventMapper employeeToEventMapper;

    @Value("${spring.kafka.topic.employee}")
    private String employeeTopic;

    private void sendEvent(Employee employee, String eventType) {
        EmployeeEvent event = employeeToEventMapper.toEvent(employee, eventType);

        Message<EmployeeEvent> message = MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, employeeTopic)
                .setHeader(KafkaHeaders.KEY, String.valueOf(employee.getId()))
                .setHeader("eventType", eventType)
                .build();

        CompletableFuture<SendResult<String, EmployeeEvent>> future = kafkaTemplate.send(message);
        future.whenComplete((result, ex) -> { ... });
    }
}
```

| Class / Annotation | Package | Purpose |
|---|---|---|
| `@Slf4j` | `lombok` | Generates `private static final Logger log` — no manual logger setup |
| `@Component` | `org.springframework.stereotype` | Registers this as a Spring bean — injectable via constructor |
| `@RequiredArgsConstructor` | `lombok` | Generates a constructor for all `final` fields — Spring uses it for dependency injection |
| `@Value` | `org.springframework.beans.factory.annotation` | Injects the topic name from `application.yml` — avoids hardcoding |
| `KafkaTemplate<K, V>` | `org.springframework.kafka.core` | Central Spring Kafka class for sending messages. Auto-configured by Spring Boot. Generic types: `K` = key (`String`), `V` = value (`EmployeeEvent`) |
| `MessageBuilder` | `org.springframework.messaging.support` | Fluent builder for `Message<T>` — sets payload and headers in one chain. Preferred over constructing `ProducerRecord` directly when using Spring's messaging abstraction |
| `KafkaHeaders` | `org.springframework.kafka.support` | Constants for Kafka-specific header keys (`TOPIC`, `KEY`) — avoids magic strings |
| `Message<T>` | `org.springframework.messaging` | Spring's generic message envelope — `KafkaTemplate.send(Message)` extracts topic, key, and custom headers automatically |
| `CompletableFuture<SendResult<K,V>>` | `java.util.concurrent` | Returned by `kafkaTemplate.send()` — non-blocking. The HTTP request thread is not blocked waiting for Kafka to acknowledge |
| `SendResult<K, V>` | `org.springframework.kafka.support` | Available in the `whenComplete` callback — wraps `RecordMetadata` giving you the topic, partition, and offset of the sent message |

---

### 5. MapStruct Mapper — `EmployeeToEventMapper`

```java
@Mapper(componentModel = "spring", imports = {EventType.class, Instant.class})
public interface EmployeeToEventMapper {

    @Mapping(target = "department",     expression = "java(mapDepartment(employee))")
    @Mapping(target = "eventType",      expression = "java(EventType.valueOf(eventType))")
    @Mapping(target = "eventTimestamp", expression = "java(Instant.now().toEpochMilli())")
    @Mapping(target = "departmentBuilder", ignore = true)
    EmployeeEvent toEvent(Employee employee, @Context String eventType);

    default DepartmentInfo mapDepartment(Employee employee) {
        return DepartmentInfo.newBuilder()
                .setId(employee.getDepartment().getId())
                .setName(employee.getDepartment().getName())
                .setLocation(employee.getDepartment().getLocation())
                .build();
    }
}
```

| Class / Annotation | Package | Purpose |
|---|---|---|
| `@Mapper(componentModel = "spring")` | `org.mapstruct` | Tells MapStruct to generate an implementation of this interface at compile time and annotate it with `@Component` — making it injectable like any other Spring bean |
| `@Mapping(target, source)` | `org.mapstruct` | Maps a specific target field from a source field by name |
| `@Mapping(target, expression)` | `org.mapstruct` | Runs arbitrary Java inline — used for `eventType` (enum conversion) and `eventTimestamp` (current time). The `imports` in `@Mapper` make `EventType` and `Instant` available without full package names |
| `@Mapping(target, ignore = true)` | `org.mapstruct` | Skips `departmentBuilder` — a helper field in the generated Avro class that MapStruct would otherwise try to populate and fail |
| `@Context` | `org.mapstruct` | Passes `eventType` as a parameter available to expressions and default methods — it's not a field on the source object, just extra input to the mapping |
| `EmployeeEvent.newBuilder()` | `org.apache.avro.specific` | The correct way to construct Avro records — the builder validates required fields and handles the `SpecificRecord` internals. Using `new EmployeeEvent(...)` directly bypasses this and can cause runtime errors |
| `DepartmentInfo.newBuilder()` | `org.apache.avro.specific` | Same pattern for the nested record |

---

### 6. Consumer — `EmployeeEventConsumer`

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class EmployeeEventConsumer {

    private final EmployeeEventDocumentRepository eventDocumentRepository;

    @KafkaListener(
            topics = "${spring.kafka.topic.employee}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(ConsumerRecord<String, EmployeeEvent> record) {
        Header header = record.headers().lastHeader("eventType");
        EmployeeEvent event = record.value();
        if (event == null) { return; }
        // map and save to MongoDB
    }
}
```

| Class / Annotation | Package | Purpose |
|---|---|---|
| `@KafkaListener` | `org.springframework.kafka.annotation` | Marks this method as a Kafka consumer. Spring creates a `KafkaMessageListenerContainer` behind the scenes that polls the topic on a background thread and invokes this method for each record |
| `ConsumerRecord<K, V>` | `org.apache.kafka.clients.consumer` | Raw Kafka record — exposes key, value, topic, partition, offset, timestamp, and headers. `V = EmployeeEvent` because `specific.avro.reader: true` tells `KafkaAvroDeserializer` to produce the generated class |
| `Header` | `org.apache.kafka.common.header` | A single Kafka header — name + raw `byte[]` value. Used to read `eventType` without deserialising the Avro payload |
| `record.headers().lastHeader("eventType")` | `org.apache.kafka.common.header.Headers` | Retrieves the most recent header with this key — `lastHeader` is used instead of `headers` in case the header was set multiple times |

---

### 7. In Tests — Testcontainers for Schema Registry

The Kafka and Schema Registry containers require extra wiring compared to Postgres and MongoDB:

```java
@Container
static final KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

@Container
static final GenericContainer<?> schemaRegistry =
        new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.6.1"))
                .withNetwork(network)
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:29092")
                .withExposedPorts(8081)
                .dependsOn(kafka);

registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
registry.add("spring.kafka.properties.schema.registry.url", () ->
        "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081));
```

| Class | Package | Purpose |
|---|---|---|
| `KafkaContainer` | `org.testcontainers.containers` | Starts a real Confluent Kafka Docker container. `getBootstrapServers()` returns the dynamically mapped host:port |
| `GenericContainer<?>` | `org.testcontainers.containers` | Used for Schema Registry since Testcontainers has no dedicated Schema Registry container class — `withEnv` and `withExposedPorts` configure it manually |
| `DockerImageName.parse(...)` | `org.testcontainers.utility` | Type-safe image name — Testcontainers validates the format before pulling |
| `Network` | `org.testcontainers.containers` | Shared Docker network — Schema Registry needs to reach Kafka by container name, which requires both to be on the same Docker network |
| `@DynamicPropertySource` | `org.springframework.test.context` | Overrides `application.yml` at test startup with container-provided URLs — no hardcoded test config |
| `getMappedPort(8081)` | `org.testcontainers.containers` | Returns the actual host port that Docker mapped to container port 8081 — different each test run |

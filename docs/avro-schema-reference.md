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
    id 'com.github.davidmc24.gradle.plugin.avro' version '1.9.1'  // generates Java from .avsc
}

repositories {
    mavenCentral()
    maven { url 'https://packages.confluent.io/maven/' }  // required — Confluent isn't on Maven Central
}

dependencies {
    implementation 'org.apache.avro:avro:1.11.3'

    // Confluent Avro serialiser/deserialiser + Schema Registry client
    implementation('io.confluent:kafka-avro-serializer:7.6.1') {
        exclude group: 'io.swagger.core.v3', module: 'swagger-annotations'  // avoids Swagger version conflict
    }
    implementation('io.confluent:kafka-schema-registry-client:7.6.1') {
        exclude group: 'io.swagger.core.v3', module: 'swagger-annotations'
    }
}

// Tell Avro plugin to generate String (not CharSequence) and keep fields private
avro {
    stringType = 'String'
    fieldVisibility = 'PRIVATE'
}

// Add generated sources to the compile path
sourceSets.main.java.srcDirs += ["$buildDir/generated-main-avro-java"]
```

The `exclude` blocks are important — `kafka-avro-serializer` transitively pulls in an older `swagger-annotations` that conflicts with SpringDoc. Without them, the build fails.

---

### 2. `application.yml` — Wiring Avro into Spring Kafka

```yaml
spring:
  kafka:
    bootstrap-servers: kafka:29092
    properties:
      schema.registry.url: http://schema-registry:8081  # shared by producer and consumer
      specific.avro.reader: true                         # deserialise into EmployeeEvent, not GenericRecord
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
```

| Setting | Why it matters |
|---|---|
| `schema.registry.url` | Both producer and consumer use this — producer to register the schema, consumer to fetch it for deserialisation |
| `specific.avro.reader: true` | Without this, the consumer gets a `GenericRecord` (basically a map). With it, you get a strongly typed `EmployeeEvent` object |
| `KafkaAvroSerializer` | Writes a 5-byte Schema Registry prefix (magic byte + schema ID) before the Avro binary payload |
| `KafkaAvroDeserializer` | Reads that prefix, fetches the schema by ID from Schema Registry, then deserialises the bytes into `EmployeeEvent` |

---

### 3. Producer

`KafkaTemplate<String, EmployeeEvent>` is all that's needed — Spring auto-configures it from `application.yml`. The key is a `String` (employee ID), the value is the generated `EmployeeEvent` Avro object.

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class EmployeeEventProducer {

    private final KafkaTemplate<String, EmployeeEvent> kafkaTemplate;

    @Value("${spring.kafka.topic.employee}")
    private String topic;

    public void send(Employee employee, String eventType) {
        EmployeeEvent event = EmployeeEvent.newBuilder()
                .setId(employee.getId())
                .setFirstName(employee.getFirstName())
                .setLastName(employee.getLastName())
                .setEmail(employee.getEmail())
                .setSalary(employee.getSalary())
                .setHireDate(employee.getHireDate())
                .setDepartment(DepartmentInfo.newBuilder()
                        .setId(employee.getDepartment().getId())
                        .setName(employee.getDepartment().getName())
                        .setLocation(employee.getDepartment().getLocation())
                        .build())
                .setEventType(EventType.valueOf(eventType))
                .setEventTimestamp(Instant.now().toEpochMilli())
                .build();

        // Attach eventType as a message header so consumers can route without deserialising
        ProducerRecord<String, EmployeeEvent> record = new ProducerRecord<>(topic, String.valueOf(employee.getId()), event);
        record.headers().add("eventType", eventType.getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send EmployeeEvent [{}] for employee id={}: {}", eventType, employee.getId(), ex.getMessage());
            } else {
                log.info("EmployeeEvent [{}] sent → topic={}, partition={}, offset={}",
                        eventType,
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
```

Key points:
- **Builder pattern** — the generated `EmployeeEvent.newBuilder()` is the correct way to construct Avro records. Don't use `new EmployeeEvent(...)` directly.
- **`whenComplete`** — non-blocking. The send is async; the callback fires on success or failure without blocking the HTTP thread.
- **Message header** — `eventType` is added as a raw byte header. This lets consumers inspect the event type without deserialising the full Avro payload.

---

### 4. Consumer

`@KafkaListener` with `ConsumerRecord<String, EmployeeEvent>` — Spring injects the fully deserialised `EmployeeEvent` object because `specific.avro.reader: true` is set.

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
        // Read eventType from header — avoids deserialising payload just to check type
        String eventType = "UNKNOWN";
        Header header = record.headers().lastHeader("eventType");
        if (header != null) {
            eventType = new String(header.value(), StandardCharsets.UTF_8);
        }

        EmployeeEvent event = record.value();
        if (event == null) {
            log.warn("Received null payload — skipping. topic={} partition={} offset={}",
                    record.topic(), record.partition(), record.offset());
            return;
        }

        // Map Avro event → MongoDB document
        EmployeeEventDocument doc = EmployeeEventDocument.builder()
                .employeeId(event.getId())
                .firstName(event.getFirstName())
                .salary(event.getSalary())           // BigDecimal — no conversion needed
                .hireDate(event.getHireDate())        // LocalDate — no conversion needed
                .departmentName(event.getDepartment().getName())
                .eventType(eventType)
                .eventTimestamp(Instant.ofEpochMilli(event.getEventTimestamp()))
                .receivedAt(Instant.now())
                .build();

        eventDocumentRepository.save(doc);
        log.info("Saved EmployeeEvent [{}] for employee id={} to MongoDB", eventType, event.getId());
    }
}
```

Key points:
- **`ConsumerRecord<String, EmployeeEvent>`** — the generic type must match. If `specific.avro.reader` is false, this would be `ConsumerRecord<String, Object>` and you'd need to cast.
- **`event.getSalary()` returns `BigDecimal` directly** — the `DecimalConversion` registered in the generated class handles the bytes → BigDecimal conversion transparently.
- **`event.getHireDate()` returns `LocalDate` directly** — same reason; `DateConversion` is registered automatically.
- **Null guard on `event`** — a tombstone message (key with null value) is valid Kafka — always guard against it.
- **`enable-auto-commit: false`** — Spring Kafka commits the offset only after the listener method returns successfully. If `save()` throws, the offset is not committed and the message is retried.

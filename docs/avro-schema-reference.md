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


# kafka-ai — Spring Boot + Kafka + Avro Demo

A full-stack Spring Boot application demonstrating event-driven architecture using Apache Kafka with Avro schema, Schema Registry, PostgreSQL, and Docker.

---

## 🏗️ Architecture Overview

```
REST API (POST /api/employees)
        │
        ▼
EmployeeService
        │  saves to DB
        ▼
PostgreSQL (employees table)
        │  sends Kafka event
        ▼
EmployeeEventProducer
        │  serializes with KafkaAvroSerializer
        │  sets "eventType" header (CREATED / UPDATED / DELETED)
        ▼
Kafka Topic: employee_topic (3 partitions)
        │  Schema Registry validates Avro schema
        ▼
EmployeeEventConsumer
        │  deserializes EmployeeEvent (SpecificRecord)
        │  maps via MapStruct (EmployeeEvent → Employee)
        ▼
PostgreSQL (updates employee fields + lastUpdated)
```

---

## 🧱 Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 4.x |
| Messaging | Apache Kafka (Confluent 7.6.1) |
| Schema | Apache Avro + Confluent Schema Registry |
| Database | PostgreSQL 16 |
| ORM | Spring Data JPA / Hibernate |
| Mapping | MapStruct 1.6.3 |
| Code Gen | Lombok |
| API Docs | SpringDoc OpenAPI 3 (Swagger UI) |
| Build | Gradle 8.14 |
| Containerisation | Docker + Docker Compose |
| Testing | JUnit 5, Testcontainers, Awaitility |

---

## 📁 Project Structure

```
src/
├── main/
│   ├── avro/
│   │   └── message.avsc              # Avro schema definition
│   ├── java/com/nik/kafka/
│   │   ├── config/
│   │   │   ├── KafkaTopicConfig.java  # Creates employee_topic
│   │   │   └── OpenApiConfig.java     # Swagger config
│   │   ├── controller/
│   │   │   ├── EmployeeController.java
│   │   │   └── DepartmentController.java
│   │   ├── service/
│   │   │   ├── EmployeeService.java
│   │   │   └── DepartmentService.java
│   │   ├── entity/
│   │   │   ├── Employee.java          # includes lastUpdated field
│   │   │   └── Department.java
│   │   ├── kafka/
│   │   │   ├── EmployeeEventProducer.java   # sends EmployeeEvent to Kafka
│   │   │   └── EmployeeEventConsumer.java   # consumes and updates DB
│   │   ├── mapstruct/
│   │   │   ├── EmployeeToEventMapper.java   # Employee → EmployeeEvent (Avro)
│   │   │   └── EmployeeEventMapper.java     # EmployeeEvent (Avro) → Employee
│   │   ├── dto/
│   │   ├── repository/
│   │   └── NikKafkaApplication.java
│   └── resources/
│       ├── application.yml
│       └── message.avsc               # copied here for classpath access
└── test/
    └── java/com/nik/kafka/
        ├── BaseIntegrationTest.java   # Testcontainers base (Postgres + Kafka)
        └── kafka/
            └── EmployeeEndToEndTest.java   # 7 end-to-end tests
```

---

## 🐳 Running the Project

**Prerequisites:** Docker Desktop running

```bash
# Start everything (builds the app, starts all services)
docker compose up --build -d

# Tear down
docker compose down

# Tear down + wipe DB data
docker compose down -v
```

### Services started by Docker Compose

| Container | Purpose | Port |
|---|---|---|
| `nik-kafka-app` | Spring Boot application | 8080 |
| `nik-postgres` | PostgreSQL database | 5432 |
| `nik-zookeeper` | Kafka coordination | 2181 |
| `nik-kafka` | Kafka broker | 9092 |
| `nik-schema-registry` | Avro Schema Registry | 8081 |
| `nik-control-center` | Confluent Kafka UI | 9021 |
| `nik-kafka-init` | Creates `employee_topic` | — |
| `nik-schema-registry-init` | Registers Avro schema | — |

---

## 🌐 URLs

| Service | URL |
|---|---|
| Swagger UI | http://localhost:8080/swagger-ui.html |
| OpenAPI JSON | http://localhost:8080/v3/api-docs |
| Employee API | http://localhost:8080/api/employees |
| Department API | http://localhost:8080/api/departments |
| Confluent Control Center | http://localhost:9021 |
| Schema Registry | http://localhost:8081 |
| Registered Schemas | http://localhost:8081/subjects |
| employee_topic Schema | http://localhost:8081/subjects/employee_topic-value/versions/latest |

---

## 📋 API Endpoints

### Departments
| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/departments` | List all departments |
| GET | `/api/departments/{id}` | Get department by ID |
| POST | `/api/departments` | Create department |
| PUT | `/api/departments/{id}` | Update department |
| DELETE | `/api/departments/{id}` | Delete department |

### Employees
| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/employees` | List all employees |
| GET | `/api/employees/{id}` | Get employee by ID |
| GET | `/api/employees/department/{id}` | Get employees by department |
| POST | `/api/employees` | Create employee (**triggers Kafka CREATED event**) |
| PUT | `/api/employees/{id}` | Update employee (**triggers Kafka UPDATED event**) |
| DELETE | `/api/employees/{id}` | Delete employee (**triggers Kafka DELETED event**) |

---

## 📨 Kafka Event Flow

### Avro Schema (`message.avsc`)
The `EmployeeEvent` Avro schema defines:
- `id`, `firstName`, `lastName`, `email` — employee fields
- `salary` — `decimal` logical type
- `hireDate` — `date` logical type
- `department` — nested `DepartmentInfo` record
- `eventType` — enum: `CREATED`, `UPDATED`, `DELETED`
- `eventTimestamp` — epoch millis

### Producer (`EmployeeEventProducer`)
- Uses **Avro SpecificRecord** (`EmployeeEvent`) generated by the Gradle Avro plugin
- Maps `Employee` entity → `EmployeeEvent` via **MapStruct** (`EmployeeToEventMapper`)
- Sets Kafka message header `eventType` = `CREATED` / `UPDATED` / `DELETED`
- Serializes with `KafkaAvroSerializer` (registers schema in Schema Registry automatically)

### Consumer (`EmployeeEventConsumer`)
- Listens on `employee_topic`, group `nik-kafka-group`
- Deserializes with `KafkaAvroDeserializer` using `specific.avro.reader=true`
- Reads `eventType` from Kafka message header
- Maps `EmployeeEvent` → `Employee` via **MapStruct** (`EmployeeEventMapper`)
- Updates the employee record in PostgreSQL including `lastUpdated = Instant.now()`
- Full error handling with try/catch — logs errors but does not block the consumer

---

## 🗄️ Database

### Tables

**`departments`**
| Column | Type | Notes |
|---|---|---|
| `id` | bigint | PK, auto-generated |
| `name` | varchar | unique |
| `location` | varchar | |

**`employees`**
| Column | Type | Notes |
|---|---|---|
| `id` | bigint | PK, auto-generated |
| `first_name` | varchar | |
| `last_name` | varchar | |
| `email` | varchar | unique |
| `salary` | numeric(38,2) | |
| `hire_date` | date | |
| `department_id` | bigint | FK → departments |
| `last_updated` | timestamptz | **set by Kafka consumer** |

> `last_updated` is only set by the Kafka consumer — not by the REST service directly. This demonstrates the event-driven update pattern.

---

## 🧪 Tests

End-to-end integration tests using **Testcontainers** (real Docker containers — no mocks):

```bash
./gradlew test --tests "com.nik.kafka.kafka.EmployeeEndToEndTest"
```

### Test Setup (`BaseIntegrationTest`)
- Starts a **real PostgreSQL 16** container
- Starts a **real Confluent Kafka 7.6.1** container
- Boots the **full Spring Boot server** on a random port
- Uses `mock://test` Schema Registry URL (Confluent mock — no real SR needed in tests)

### Test Cases

| Test | What it verifies |
|---|---|
| `createEmployee_producerSendsEvent_consumerUpdatesDB` | POST → Kafka CREATED → consumer sets `lastUpdated` |
| `updateEmployee_producerSendsEvent_consumerUpdatesAllFieldsInDB` | PUT → Kafka UPDATED → consumer updates all fields via MapStruct |
| `deleteEmployee_endpointReturns204_employeeRemovedFromDB` | DELETE → employee removed from DB |
| `createEmployee_kafkaMessageHasCorrectEventTypeHeader` | `eventType` header set correctly by producer |
| `createEmployee_duplicateEmail_returnsError` | DB unique constraint enforced → error response |
| `createEmployee_nonExistentDepartment_returnsError` | Non-existent department ID → error response |
| `getEmployeesByDepartment_returnsOnlyThatDepartmentsEmployees` | GET filter by department returns correct count |

---

## 🔑 Key Design Decisions

### Why SpecificRecord instead of GenericRecord?
The Gradle Avro plugin (`com.github.davidmc24.gradle.plugin.avro`) generates `EmployeeEvent.java` from `message.avsc` at build time. This gives:
- **Type safety** — no raw `.put("fieldName", value)` calls
- **IDE support** — auto-complete on field names
- **Schema evolution** — compiler catches breaking changes

### Why MapStruct?
MapStruct generates the mapping code at compile time (zero reflection overhead at runtime). Two mappers:
- `EmployeeToEventMapper` — `Employee` → `EmployeeEvent` (used by producer)
- `EmployeeEventMapper` — `EmployeeEvent` → `Employee` (used by consumer)

### Why `lastUpdated` is set by the consumer, not the service?
This demonstrates the **event-driven update pattern**: the source of truth for `lastUpdated` is the Kafka event, not the HTTP request. The consumer acts as an independent downstream processor that reflects what was committed to Kafka.

### Why `mock://test` in tests?
Confluent's `KafkaAvroSerializer`/`KafkaAvroDeserializer` support `mock://` URLs natively. This means tests get full Avro serialisation/deserialisation without needing a real Schema Registry container.

---

## 🔧 Configuration

Key settings in `application.yml`:

```yaml
spring:
  kafka:
    bootstrap-servers: kafka:29092          # overridden in Docker via env var
    properties:
      schema.registry.url: http://schema-registry:8081
      specific.avro.reader: true            # use generated EmployeeEvent class
    producer:
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
    consumer:
      group-id: nik-kafka-group
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      enable-auto-commit: false
```

Docker Compose overrides for the app container:
```yaml
SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_URL: http://schema-registry:8081
```


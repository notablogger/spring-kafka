# kafka-ai — Spring Boot + Kafka + Avro + MongoDB

Event-driven architecture demo: REST API → Kafka (Avro) → MongoDB event log.

---

## Architecture

```
POST/PUT/DELETE → EmployeeService → Postgres (source of truth)
                        │
                 EmployeeEventProducer → Kafka (Avro)
                                              │
                                     EmployeeEventConsumer
                                              │
                                          MongoDB
                                     (employee_events)

GET → EmployeeService → MongoDB (latest event per employee)
```

- **PostgreSQL** — source of truth for writes (create / update / delete)
- **Kafka + Avro** — employee events streamed on every mutation (CREATED / UPDATED / DELETED)
- **MongoDB** — append-only event log; all GET reads are served from here (latest event per employee, DELETED records excluded)

---

## Stack

| Layer | Technology |
|---|---|
| Runtime | Java 21, Spring Boot 4 |
| REST | Spring MVC + SpringDoc (Swagger) |
| Messaging | Apache Kafka + Confluent Schema Registry (Avro) |
| Source DB | PostgreSQL 16 |
| Event log | MongoDB 7 |
| Mapping | MapStruct |

---

## Running locally

```bash
docker-compose up -d
```

Services:

| Service | URL |
|---|---|
| App | http://localhost:8080 |
| Swagger | http://localhost:8080/swagger-ui.html |
| Control Center | http://localhost:9021 |
| Schema Registry | http://localhost:8081 |
| PostgreSQL | localhost:5432 |
| MongoDB | localhost:27017 |

---

## API Endpoints

### Departments

| Method | Path | Description |
|---|---|---|
| GET | `/api/departments` | List all departments |
| GET | `/api/departments/{id}` | Get department by ID |
| POST | `/api/departments` | Create a department |
| PUT | `/api/departments/{id}` | Update a department |
| DELETE | `/api/departments/{id}` | Delete a department |

### Employees

| Method | Path | Source | Description |
|---|---|---|---|
| GET | `/api/employees` | **MongoDB** | List all active employees (latest event, DELETED excluded) |
| GET | `/api/employees/{id}` | **MongoDB** | Get employee by ID (latest event) |
| GET | `/api/employees/department/{departmentId}` | **MongoDB** | List employees in a department |
| POST | `/api/employees` | **Postgres** | Create employee → fires CREATED Kafka event |
| PUT | `/api/employees/{id}` | **Postgres** | Update employee → fires UPDATED Kafka event |
| DELETE | `/api/employees/{id}` | **Postgres** | Delete employee → fires DELETED Kafka event |

---

## Data Flow (step by step)

1. Client calls `POST /api/employees`
2. `EmployeeService` saves to **PostgreSQL**
3. `EmployeeEventProducer` sends an **Avro** `EmployeeEvent` to the `employee_topic` Kafka topic
4. `EmployeeEventConsumer` consumes the event and saves an `EmployeeEventDocument` to **MongoDB**
5. Client calls `GET /api/employees` — served directly from **MongoDB** (most recent event per employee)

---

## Key Design Decisions

- **GET from MongoDB** — reads are served from the event log, not Postgres. Each GET returns the latest non-deleted event snapshot per employee, including `eventType` and `eventTimestamp`.
- **Avro SpecificRecord** generated from `message.avsc` via Gradle plugin. Logical types produce `BigDecimal` (decimal) and `LocalDate` (date) directly — no manual conversion needed.
- **Consumer appends only** — it never updates PostgreSQL. Postgres is exclusively owned by the write (REST) layer.
- **`mock://` Schema Registry** — for local development without a running Schema Registry, set `spring.kafka.properties.schema.registry.url=mock://test`.

---

## Employee Response Fields

| Field | Source | Notes |
|---|---|---|
| `id` | MongoDB | Employee ID from Postgres |
| `firstName` | MongoDB | From event snapshot |
| `lastName` | MongoDB | From event snapshot |
| `email` | MongoDB | From event snapshot |
| `salary` | MongoDB | From event snapshot |
| `hireDate` | MongoDB | From event snapshot |
| `departmentName` | MongoDB | From event snapshot |
| `departmentLocation` | MongoDB | From event snapshot |
| `eventType` | MongoDB | `CREATED`, `UPDATED`, or `DELETED` |
| `eventTimestamp` | MongoDB | When the Kafka event was produced |

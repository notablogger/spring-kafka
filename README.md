# kafka-ai — Learning Kafka Integration with AI

> 🚀 **A personal learning project** — building a production-style event-driven system from scratch, using GitHub Copilot as a pair-programmer, with a plan to rebuild the same thing in multiple languages.

---

## 🎯 The Goal

I'm on a mission to deeply understand **Kafka integration patterns** by building the same event-driven application in multiple programming languages — starting with Java/Spring Boot, then Go, Python, and Node.js.

Rather than spending weeks on boilerplate, I used **GitHub Copilot (AI)** to accelerate every layer of the build — letting me focus on *understanding the architecture* rather than fighting the setup. Every decision was mine; AI was the hands.

---

## 🤖 How AI Helped Me Learn

This isn't a "let AI do it for me" project. It's a **learning accelerator**:

- I described *what* I wanted to build — AI generated the scaffold
- I asked *why* things worked a certain way — AI explained
- When something broke — I diagnosed it with AI, understood the root cause, and fixed it
- When the architecture needed to change (e.g. reads from MongoDB instead of Postgres) — I made that call, AI refactored

**The result:** I learned Kafka producers/consumers, Avro schema design, CQRS patterns, and Spring Boot integration in days, not weeks.

📁 See the [`AI docs/`](./AI%20docs/) folder for the full journey — every conversation, every fix, every prompt.

---

## 🏗️ Architecture

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

**The CQRS split:**
- **Writes** → Postgres + Kafka event fired on every mutation
- **Reads** → MongoDB event log (latest non-deleted snapshot per employee)

---

## 🛠️ Stack

| Layer | Technology |
|---|---|
| Runtime | Java 21, Spring Boot 4 |
| REST | Spring MVC + SpringDoc (Swagger) |
| Messaging | Apache Kafka + Confluent Schema Registry (Avro) |
| Source DB | PostgreSQL 16 |
| Event log | MongoDB 7 |
| Mapping | MapStruct |
| Containers | Docker Compose |

---

## 🚀 Running Locally

```bash
docker-compose up -d
```

| Service | URL |
|---|---|
| App | http://localhost:8080 |
| Swagger UI | http://localhost:8080/swagger-ui.html |
| Confluent Control Center | http://localhost:9021 |
| Schema Registry | http://localhost:8081 |
| PostgreSQL | localhost:5432 |
| MongoDB | localhost:27017 |

---

## 📡 API Endpoints

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
| GET | `/api/employees` | **MongoDB** | List all active employees |
| GET | `/api/employees/{id}` | **MongoDB** | Get employee by ID |
| GET | `/api/employees/department/{id}` | **MongoDB** | Employees in a department |
| POST | `/api/employees` | **Postgres** | Create → fires CREATED event |
| PUT | `/api/employees/{id}` | **Postgres** | Update → fires UPDATED event |
| DELETE | `/api/employees/{id}` | **Postgres** | Delete → fires DELETED event |

---

## 🔁 Data Flow (Step by Step)

1. `POST /api/employees` → saved to **PostgreSQL**
2. `EmployeeEventProducer` sends an **Avro** message to `employee_topic`
3. `EmployeeEventConsumer` picks it up → saves `EmployeeEventDocument` to **MongoDB**
4. `GET /api/employees` → reads directly from **MongoDB** (latest event snapshot)

---

## 🌍 Multi-Language Roadmap

The same application will be rebuilt in each language to compare patterns, boilerplate, and DX:

| Language | Framework | Status |
|---|---|---|
| ☕ Java | Spring Boot 4 | ✅ Complete |
| 🐹 Go | Fiber + confluent-kafka-go | 🔜 Next |
| 🐍 Python | FastAPI + confluent-kafka | 🔜 Planned |
| 🟨 Node.js | NestJS + kafkajs | 🔜 Planned |

Each version will live in its own folder/branch with the same REST contract and Kafka topic structure.

---

## 📁 AI Docs

| File | What's inside |
|---|---|
| [`how-ai-helped-me-build-this.md`](./AI%20docs/how-ai-helped-me-build-this.md) | What AI generated, what I decided, and how I learned |
| [`conversation-log.md`](./AI%20docs/conversation-log.md) | Full table of every prompt → AI action |
| [`prompt-new-language.md`](./AI%20docs/prompt-new-language.md) | Reusable prompt to rebuild in any language + full prompt history |

---

## 💡 Key Design Decisions

- **GET from MongoDB** — reads are served from the event log. Every GET returns the latest non-deleted event snapshot, including `eventType` and `eventTimestamp`
- **Avro logical types** — `BigDecimal` (decimal) and `LocalDate` (date) mapped natively via Avro logical types — no manual conversion
- **Consumer appends only** — never writes back to Postgres. Postgres is exclusively owned by the write layer
- **Proper error handling** — `@RestControllerAdvice` returns clean JSON errors with correct HTTP status codes (404, 400, 500)
- **Input validation** — all DTOs validated with `@NotBlank`, `@Email`, `@DecimalMin` etc.

---

## 🧠 What I Learned

- How Kafka producers and consumers work at the code level
- Avro schema design with logical types (decimal, date, nested records, enums)
- CQRS — separating read and write models across different databases
- How to use AI as a learning tool, not a shortcut
- Schema Registry and why it matters for contract enforcement

---

## 🧪 Running Tests

Integration tests spin up **real containers** via Testcontainers — no mocks, no stubs.

```bash
./gradlew test
```

### What the tests cover

| Test | What it verifies |
|---|---|
| `createDepartment_validRequest_returns201` | POST creates dept, returns 201 + body |
| `getAllDepartments_returns200WithList` | GET returns list containing created dept |
| `getDepartmentById_existingId_returns200` | GET by ID returns correct body |
| `getDepartmentById_nonExistentId_returns404` | Missing ID returns 404, not 500 |
| `updateDepartment_validRequest_returns200` | PUT updates name/location |
| `createDepartment_missingName_returns400` | Validation rejects blank name |
| `createEmployee_validRequest_returns201AndSavesToPostgres` | POST creates employee in Postgres |
| `createEmployee_kafkaProducerFiresEvent_consumerSavesToMongoDB` | Kafka CREATED event lands in MongoDB with correct fields |
| `getEmployeeById_readsFromMongoDB_returnsCorrectSnapshot` | GET reads from MongoDB, returns eventType + eventTimestamp |
| `getAllEmployees_readsFromMongoDB_returnsNonEmptyList` | GET all reads from MongoDB |
| `getEmployeesByDepartment_readsFromMongoDB_returnsFilteredList` | Department filter works from MongoDB |
| `updateEmployee_kafkaProducerFiresUpdatedEvent_consumerSavesToMongoDB` | Kafka UPDATED event lands in MongoDB |
| `getEmployeeById_afterUpdate_returnsUpdatedSnapshot` | GET returns latest (UPDATED) snapshot |
| `deleteEmployee_removedFromPostgres_deletedEventInMongoDB_getReturns404` | DELETE fires DELETED event; GET returns 404 |
| `createEmployee_invalidEmail_returns400` | Email validation rejects bad format |
| `createEmployee_zeroSalary_returns400` | Salary validation rejects zero |
| `createEmployee_nonExistentDepartment_returns404` | Unknown dept ID returns 404 |
| `deleteDepartment_validId_returns204` | DELETE returns 204 |

### Infrastructure spun up per test run

| Container | Image | Purpose |
|---|---|---|
| PostgreSQL | `postgres:16` | Write DB for employees/departments |
| Kafka | `confluentinc/cp-kafka:7.6.1` | Event broker |
| Schema Registry | `confluentinc/cp-schema-registry:7.6.1` | Avro schema validation (real, not mock) |
| MongoDB | `mongo:7` | Event log / read model |

> ⏱ Expect the full suite to take **2–3 minutes** — the Kafka flow tests use `Awaitility` and wait up to 20 seconds for the async consumer to process each event.

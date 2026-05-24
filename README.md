# kafka-ai — Learning to Work with AI, One Language at a Time

> 🧠 **The real goal isn't Kafka. It's learning how to use AI effectively** — understanding how it thinks, how it responds to different prompts, and how to get real production-quality work done through conversation alone.
>
> Kafka + multi-language is the vehicle. AI is the subject.

---

## 🎯 The Mission

Most people use AI to autocomplete lines of code. I'm using it differently — as a **study in how AI behaves**.

This project is an experiment: can I build a production-grade event-driven system, in multiple programming languages, purely through conversation with an AI agent? No tutorials. No copy-paste. Just prompts.

Three things I'm learning at once:
1. **How AI works** — how it interprets vague vs precise prompts, when it makes decisions vs asks questions, how it handles ambiguity, bugs, and architectural pivots
2. **Kafka integration patterns** — producers, consumers, Avro, Schema Registry, CQRS
3. **Different programming languages** — same problem, different paradigms, different ecosystems

---

## 🤖 What I'm Learning About AI

This isn't about the code. It's about the tool.

- **How specific do prompts need to be?** — Some prompts were one line. Some needed detail. Watching what AI does with each taught me where precision matters.
- **When does AI decide vs ask?** — AI never asked for clarification when it could act. Understanding that boundary helps you write better prompts.
- **How does AI handle failure?** — When tests failed, AI diagnosed iteratively. Watching it work through the problem taught me how it reasons.
- **When do you override AI?** — Every architectural decision (CQRS reads from MongoDB, the multi-language plan) was mine. AI executed. Knowing when to take the wheel is the skill.
- **How do you audit AI output?** — Asking "does this follow best practices?" found 6 real bugs. That prompt is now part of my workflow.

📁 The full story is in [`AI docs/`](./AI%20docs/) — every prompt, every response, every decision.

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

## 🛠️ Stack (Java Version)

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

Same problem. Same architecture. Different language each time. The goal is to see how AI adapts — and how the languages compare.

| Language | Framework | Status |
|---|---|---|
| ☕ Java | Spring Boot 4 | ✅ Complete |
| 🐹 Go | Fiber + confluent-kafka-go | 🔜 Next |
| 🐍 Python | FastAPI + confluent-kafka | 🔜 Planned |
| 🟨 Node.js | NestJS + kafkajs | 🔜 Planned |

Each version uses the same REST contract, same Kafka topic, same Avro schema. The prompt to build each one is in [`AI docs/prompt-new-language.md`](./AI%20docs/prompt-new-language.md).

---

## 📁 AI Docs

| File | What's inside |
|---|---|
| [`how-ai-helped-me-build-this.md`](./AI%20docs/how-ai-helped-me-build-this.md) | What AI generated, what I decided, and what I actually learned |
| [`conversation-log.md`](./AI%20docs/conversation-log.md) | Every prompt, what AI understood, what it did |
| [`prompt-new-language.md`](./AI%20docs/prompt-new-language.md) | Reusable prompt to rebuild in any language |

---

## 📖 Technical Reference Docs

Deep-dive docs on every infrastructure component and the Avro schema — written for anyone who wants to understand *why* things are configured the way they are, not just what they are.

| File | What's inside |
|---|---|
| [`docs/avro-schema-reference.md`](./docs/avro-schema-reference.md) | Full schema breakdown — why `decimal` not `float`, why `date` not `string`, why department is a nested record, code generation pipeline, schema evolution |
| [`docs/kafka-config-reference.md`](./docs/kafka-config-reference.md) | Every Kafka service explained — broker, Zookeeper, Schema Registry, Control Center, the two-listener pattern, startup order |
| [`docs/mongo-config-reference.md`](./docs/mongo-config-reference.md) | MongoDB setup, the `employee_events` collection schema, read path logic, how DELETED employees are filtered |
| [`docs/postgres-config-reference.md`](./docs/postgres-config-reference.md) | Postgres setup, JPA/Hibernate settings, entity schema, `LazyInitializationException` — what it is and how it was fixed |

---

## 💡 Key Technical Decisions

- **GET from MongoDB** — reads from the event log. Latest non-deleted snapshot per employee, including `eventType` and `eventTimestamp`
- **Avro logical types** — `BigDecimal` and `LocalDate` via Avro logical types — no manual conversion
- **Consumer appends only** — never writes back to Postgres
- **Proper error handling** — `@RestControllerAdvice` with correct HTTP status codes
- **Input validation** — `@NotBlank`, `@Email`, `@DecimalMin` on all DTOs

---

## 🧪 Running Tests

Integration tests spin up **real containers** — no mocks.

```bash
./gradlew test
```

| Container | Image | Purpose |
|---|---|---|
| PostgreSQL | `postgres:16` | Write DB |
| Kafka | `confluentinc/cp-kafka:7.6.1` | Event broker |
| Schema Registry | `confluentinc/cp-schema-registry:7.6.1` | Avro validation |
| MongoDB | `mongo:7` | Event log / read model |

> ⏱ Full suite takes **2–3 minutes** — Kafka flow tests use Awaitility and wait up to 20 seconds for the async consumer.

---

## 🧠 What I'm Taking Away

| Skill | How This Project Taught It |
|---|---|
| Using AI effectively | Watched it reason, fail, recover, and pivot across 10+ sessions |
| Prompt engineering | Learned what to be specific about and what to leave to AI |
| Kafka integration | Built producer, consumer, Avro, Schema Registry from scratch |
| CQRS pattern | Made the architectural call to split reads and writes across databases |
| Code quality habits | Asking AI to audit caught 6 production bugs I'd have missed |
| Multi-language thinking | Same problem in Go/Python/Node will expose what's language vs what's pattern |

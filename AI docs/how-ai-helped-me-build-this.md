# How AI Helped Me Build This

This project was built through conversation with GitHub Copilot inside JetBrains IDE. No boilerplate was written by hand. But more importantly — **I learned by doing it this way, not despite it**.

---

## The Learning Philosophy

Using AI to build doesn't mean you skip the learning. It means you spend your time on **architecture decisions, debugging, and understanding** — not on remembering annotations or fighting Maven dependency trees.

Every piece of code AI generated, I read, questioned, and understood before moving on. When something broke, I didn't just paste the error — I diagnosed it first, then used AI to confirm or correct my thinking.

---

## What AI Generated

### Project Scaffold
The entire `build.gradle`, package structure, Docker Compose, and `application.yml` were generated from a single high-level description. This saved ~2 hours of setup and let me go straight to learning the actual patterns.

### Domain Model
AI designed `Employee`, `Department`, and `EmployeeEventDocument` entities — including JPA annotations, Lombok, MongoDB document mappings, and the relationship between them.

### Avro Schema
`message.avsc` was generated with correct Avro logical types:
- `decimal` for salary (with precision/scale)
- `date` for hire date
- Nested `DepartmentInfo` record
- `EventType` enum

This taught me how Avro logical types map to Java types — something that takes hours to figure out from docs alone.

### Kafka Producer & Consumer
`EmployeeEventProducer` and `EmployeeEventConsumer` were fully generated with:
- Kafka message headers for `eventType`
- `CompletableFuture`-based async send with logging callbacks
- Consumer extracting headers and persisting to MongoDB

### MapStruct Mapper
`EmployeeToEventMapper` was generated with `@Context` for passing the event type, expression mappings for `EventType.valueOf()` and `Instant.now()`, and the nested `DepartmentInfo` builder — a pattern I wouldn't have discovered quickly on my own.

---

## Key Moments Where I Made the Calls

### The CQRS Pivot
Midway through, I decided: **GET endpoints should read from MongoDB, not Postgres**. This is a real architectural pattern (CQRS). AI refactored the entire `EmployeeService` in one pass — but the decision was mine, based on understanding what the event log was for.

### Standards Audit
I asked AI to audit the project against best practices. It found 6 real issues — a wrong Kafka port, missing validation, a lazy loading bug, wrong starter dependency names. I understood each one before accepting the fix.

---

## What I Actually Learned

| Topic | How I Learned It |
|---|---|
| Kafka producers/consumers | Built one, watched it break, fixed it |
| Avro logical types | Read the generated code, understood the mapping |
| CQRS pattern | Made the architectural decision myself, saw it implemented |
| Schema Registry | Understood why it exists when consumer deserialization was set up |
| Spring Boot transaction management | Hit the `LazyInitializationException`, understood why `@Transactional(readOnly=true)` fixes it |
| Proper REST error handling | Saw 500s where 404s should be, fixed it with `@RestControllerAdvice` |

---

## The Multi-Language Plan

Now that I understand the Kafka integration patterns in Java, I'll rebuild the **exact same application** in Go, Python, and Node.js — using the same approach. Same architecture, same Avro schema, same Kafka topic. Different language, different patterns, same understanding built each time.

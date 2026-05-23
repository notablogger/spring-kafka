# How AI Helped Me Build This

This project was built entirely through conversation with GitHub Copilot (GPT-4 class model) inside JetBrains IDE. No boilerplate was written by hand.

---

## What Was Built

A production-style, event-driven Spring Boot application with:

- REST API (Spring MVC) with Swagger docs
- PostgreSQL as the write source of truth
- Apache Kafka with Avro serialisation and Schema Registry
- MongoDB as a read-optimised event log
- MapStruct for entity → Avro mapping
- Docker Compose for the full local stack

---

## How AI Was Used

### 1. Project Scaffolding
Rather than using Spring Initializr manually, the entire `build.gradle`, project structure, and all base classes were generated in one pass from a high-level description of the architecture.

### 2. Domain Modelling
AI designed the entity model (`Employee`, `Department`, `EmployeeEventDocument`), DTOs (`EmployeeRequest`, `EmployeeResponse`), and their relationships — including JPA annotations, Lombok, and MongoDB document mappings.

### 3. Avro Schema Design
The `message.avsc` schema was generated with correct Avro logical types — `decimal` for salary, `date` for hire date, nested `DepartmentInfo` record, and an `EventType` enum — all in one shot.

### 4. Kafka Producer & Consumer
`EmployeeEventProducer` and `EmployeeEventConsumer` were fully generated, including:
- Kafka message headers for `eventType`
- CompletableFuture-based async send with logging
- Consumer extracting headers and persisting to MongoDB

### 5. MapStruct Mapper
`EmployeeToEventMapper` was generated with `@Context` for the event type string, expression mappings for `EventType.valueOf()` and `Instant.now()`, and the nested `DepartmentInfo` builder.

### 6. Architectural Pivot (key moment)
Midway through, the architecture was deliberately changed: **GET endpoints were redirected from Postgres to MongoDB**. AI refactored `EmployeeService` to:
- Stream all MongoDB event documents
- Deduplicate to the latest event per employee ID
- Filter out `DELETED` records
- Serve reads from the event log

This was done in a single instruction with zero manual code changes.

### 7. Documentation
README and all AI docs were written by AI based on the actual code state — not a template.

---

## Key Takeaway

AI acted as a senior engineer pair-programmer. The human provided intent and architecture direction; AI handled implementation, refactoring, and documentation. The entire project was built through natural language alone.


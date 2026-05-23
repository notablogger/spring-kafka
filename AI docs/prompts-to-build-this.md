# Prompts to Build This Project

A minimal set of prompts needed to reproduce this project from scratch using AI.

---

## 1. Initial Project Generation

> Build a Spring Boot 4 / Java 21 event-driven app called `kafka-ai`.
>
> - REST API with CRUD for `Employee` and `Department`
> - PostgreSQL as the write database (JPA/Hibernate)
> - Kafka producer that fires an Avro event on every employee create, update, delete
> - Kafka consumer that saves each event as a document in MongoDB
> - Avro schema with logical types: decimal for salary, date for hire date, nested department record, EventType enum
> - MapStruct to map Employee entity → Avro EmployeeEvent
> - Swagger/OpenAPI docs
> - Docker Compose with Postgres, Kafka, Schema Registry, MongoDB, Control Center
> - Lombok throughout

---

## 2. Architectural Change — Reads from MongoDB

> Change the GET endpoints in EmployeeController / EmployeeService so they read from MongoDB, not Postgres.
> Use the latest event document per employee. Exclude DELETED employees from results.
> Add eventType and eventTimestamp to the response DTO.

---

## 3. Documentation

> Rewrite the README to reflect the current architecture.
> Include: architecture diagram, stack table, API endpoint table (with which DB each method uses), step-by-step data flow, key design decisions, and a response field reference table.

---

## 4. AI Docs Folder

> Add an "AI docs" folder with three pages:
> 1. How AI helped me build this
> 2. A conversation log table showing each prompt, what AI understood, and what it did
> 3. The prompts needed to build this project (concise, not too detailed)

---

## Tips

- Be specific about the tech stack versions upfront (Spring Boot 4, Java 21, Kafka 7.6.1, etc.)
- State architectural intent, not implementation detail — let AI decide the code structure
- One concern per prompt — don't bundle unrelated changes
- If something fails, paste the error and say "fix this" rather than explaining the fix yourself


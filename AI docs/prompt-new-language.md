# Prompt: Build This Project in a New Language

Copy and paste this prompt to any AI agent to rebuild the same project in a different language.

---

## The Prompt

> Before you start, ask me which programming language and framework I want to use for this project.
>
> Once I confirm the language, build an event-driven application called `kafka-ai-[language]` with the following functionality:
>
> **Domain**
> - Two resources: `Employee` and `Department`
> - `Employee` belongs to one `Department`
> - `Employee` fields: id, firstName, lastName, email, salary (decimal), hireDate (date), department
> - `Department` fields: id, name, location
>
> **REST API**
> - Full CRUD for both `Employee` and `Department`
> - Input validation on all create/update endpoints (required fields, valid email, salary > 0)
> - Proper HTTP status codes: 201 for create, 204 for delete, 404 when not found
> - All errors returned as structured JSON with timestamp, status, and message
> - API documentation (Swagger/OpenAPI or equivalent for the chosen language)
>
> **Write path (Postgres)**
> - All create, update, delete operations persist to PostgreSQL
> - `Department` is the parent — always created before employees
>
> **Kafka event streaming**
> - Every employee mutation (create, update, delete) fires a Kafka event to a topic called `employee_topic`
> - Event payload must include: all employee fields, department name and location, event type (CREATED/UPDATED/DELETED), event timestamp
> - Use Avro serialisation with a Schema Registry
> - Avro schema must use correct logical types: decimal for salary, date for hire date
> - Event type must be an Avro enum: CREATED, UPDATED, DELETED
> - Department must be a nested Avro record inside the event
> - Include an `eventType` message header on every Kafka message
>
> **Kafka consumer → MongoDB**
> - A consumer listens to `employee_topic`
> - On each message, save the full event as a document to MongoDB collection `employee_events`
> - Document must include: all event fields + receivedAt timestamp
>
> **Read path (MongoDB)**
> - All GET endpoints read from MongoDB, not Postgres
> - `GET /employees` and `GET /employees/{id}` return the latest event snapshot per employee
> - Employees with a DELETED event must not appear in GET responses
> - `GET /employees/department/{id}` filters by department
>
> **Infrastructure (Docker Compose)**
> - PostgreSQL
> - Kafka broker
> - Confluent Schema Registry
> - Confluent Control Center (Kafka UI)
> - MongoDB
> - The application itself
> - All services should have health checks and correct startup ordering
>
> **Code quality**
> - Use the idiomatic patterns of the chosen language
> - Separate concerns: controller/handler → service → repository
> - All configuration (ports, credentials, topic names) via environment variables or config files — no hardcoded values
> - Structured logging on producer send (success + failure) and consumer receive
>
> **Testing**
> - Full integration test suite using real containers (no mocks)
> - Spin up PostgreSQL, Kafka, Schema Registry, and MongoDB as real containers for tests
> - Test all CRUD endpoints for both Employee and Department
> - Verify the full Kafka flow — producer fires event, consumer saves to MongoDB, verified via GET endpoints only (no direct DB access in tests)
> - Test correct HTTP status codes: 201, 204, 400, 404
> - Test input validation errors return 400
> - Use Awaitility (or equivalent) to wait for async Kafka consumer to process before asserting
> 
> Once the language is confirmed, pick the most appropriate libraries/frameworks for that language to fulfil each requirement above.
>
> **Documentation**
> After the project is built, generate the following reference docs inside a `docs/` folder:
>
> - `kafka-config-reference.md` — document every Kafka-related Docker Compose service (Zookeeper, Kafka broker, Schema Registry, Control Center, kafka-init, schema-registry-init) and every setting in the `kafka` section of the app config. For each setting, include: the config key, its value, and a plain-English explanation of what it does. Also include a startup order diagram and an explanation of the two-listener pattern.
>
> - `mongo-config-reference.md` — document the MongoDB Docker Compose service and the `spring.data.mongodb` app config. Include: the collection schema (all fields, their types, and where they come from), the read path logic (how the latest event snapshot is derived and how DELETED employees are excluded), the Java entity and repository setup, and how to wipe data for a clean run.
>
> - `postgres-config-reference.md` — document the PostgreSQL Docker Compose service and the `spring.datasource` / `spring.jpa` app config. Include: the database name, credentials, JPA/Hibernate settings (ddl-auto, dialect, show-sql), and the role Postgres plays in the write path.
>
> - `avro-schema-reference.md` — document the Avro schema file (`.avsc`). Include: a field-by-field breakdown with the Avro type, the generated Java type, and why that type was chosen. Explain the three non-obvious choices in depth: why `decimal` logical type is used for salary (not float/double), why `date` logical type is used for hireDate (not string), and why department is a nested record (event-carried state transfer pattern). Also cover: how code generation works (the `.avsc` → generated Java classes pipeline), how schema registration works at startup, and a section on schema evolution with a compatibility table.

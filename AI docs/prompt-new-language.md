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
> Once the language is confirmed, pick the most appropriate libraries/frameworks for that language to fulfil each requirement above.



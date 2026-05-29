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
> - `getting-started.md` — a project setup guide aimed at someone coming from Spring Boot / Java. Include: the equivalent of Spring Initializr for this language (how to initialise a project, the tool/command used), a table mapping Spring Initializr concepts (group/artifact, Java version, dependency checkboxes) to the language equivalent, the full stack decision table (framework, ORM, MongoDB driver, Kafka client, config, migrations, logging, package manager, test HTTP client — chosen option + alternatives), step-by-step from zero to a running app (install tools → init project → add dependencies → create folder structure → minimum working endpoint → config setup → DB migrations → run locally → run tests), a common commands cheat sheet (add dep, sync, run app, run tests, generate migration), and a "what to read next" table linking to the other docs in the folder. This doc is the entry point for any new developer on the project.
>
> - `kafka-config-reference.md` — document every Kafka-related Docker Compose service (Zookeeper, Kafka broker, Schema Registry, Control Center, kafka-init, schema-registry-init) and every setting in the `kafka` section of the app config. For each setting, include: the config key, its value, and a plain-English explanation of what it does. Also include a startup order diagram and an explanation of the two-listener pattern. Add a **Spring Integration** section covering: all dependencies with purpose, `KafkaTopicConfig` (`NewTopic`, `TopicBuilder`), the producer (`KafkaTemplate`, `MessageBuilder`, `KafkaHeaders`, `Message<T>`, `CompletableFuture`, `SendResult`), the MapStruct mapper (`@Mapper`, `@Mapping`, `@Context`), and the consumer (`@KafkaListener`, `ConsumerRecord`, `Header`). For every class and annotation include its package and a one-line explanation of what it does.
>
> - `mongo-config-reference.md` — document the MongoDB Docker Compose service and the `spring.data.mongodb` app config. Include: the collection schema, the read path logic, the Java entity and repository setup, and how to wipe data. Add a **Spring Integration** section covering: all dependencies with purpose, the `@Document` entity (`@Document`, `@Id`, `BigDecimal`, `LocalDate`, `Instant`, all Lombok annotations), the `MongoRepository` with method name queries, the consumer write path (`save()`, `Instant.ofEpochMilli`, `Instant.now()`), the read path stream logic (`Collectors.toMap` with merge function, `Instant.isAfter`), and the Testcontainers setup (`MongoDBContainer`, `@DynamicPropertySource`). For every class and annotation include its package and a one-line explanation.
>
> - `postgres-config-reference.md` — document the PostgreSQL Docker Compose service and the `spring.datasource` / `spring.jpa` app config. Include: the database name, credentials, JPA/Hibernate settings, and the role Postgres plays in the write path. Add a **Spring Integration** section covering: all dependencies with purpose, JPA entities (all `jakarta.persistence` annotations, all Lombok annotations), Spring Data JPA repositories (`JpaRepository`, `@Repository`, method name queries, `Optional<T>`), the service layer (`@Service`, `@Transactional`, `@Transactional(readOnly=true)` and why it matters for lazy loading), error handling (`@RestControllerAdvice`, `@ExceptionHandler`, `ResourceNotFoundException`, `MethodArgumentNotValidException`, `ResponseEntity`, `HttpStatus`), DTO validation (`@NotBlank`, `@NotNull`, `@Email`, `@DecimalMin`, `@Valid`, `@Schema`), and controllers (`@RestController`, `@RequestMapping`, `@PathVariable`, `@RequestBody`, all HTTP method annotations, `@Tag`, `@Operation`, `@ApiResponse`, `@Parameter`). For every class and annotation include its package and a one-line explanation.
>
> - `avro-schema-reference.md` — document the Avro schema file. Include field breakdown, the three non-obvious type choices, code generation pipeline, schema registration, schema evolution table, sample message, and a **Spring Integration** section covering: all dependencies and the Gradle plugin (`avro` plugin, `kafka-avro-serializer`, `kafka-schema-registry-client`, `avro` lib, why the Confluent Maven repo is needed, the `exclude` blocks and why), generated classes (`EmployeeEvent`, `DepartmentInfo`, `EventType`, `SpecificRecordBase`, `DecimalConversion`, `DateConversion`), `application.yml` serialiser settings (`KafkaAvroSerializer`, `KafkaAvroDeserializer`, `specific.avro.reader`, `StringSerializer`, `StringDeserializer`), the producer (`KafkaTemplate`, `MessageBuilder`, `KafkaHeaders`, `Message<T>`, `CompletableFuture`, `SendResult`), the MapStruct mapper (`@Mapper`, `@Mapping`, `@Context`, `EmployeeEvent.newBuilder()`), the consumer (`@KafkaListener`, `ConsumerRecord`, `Header`), and Testcontainers (`KafkaContainer`, `GenericContainer`, `DockerImageName`, `Network`, `@DynamicPropertySource`, `getMappedPort`). For every class and annotation include its package and a one-line explanation.
>
> - `build-reference.md` — document the `build.gradle` file in full. Include: every plugin (id, version, purpose), project coordinates (group, version), the Java toolchain declaration, all repository entries and why the Confluent repo is needed separately, every dependency with its scope (`implementation`, `compileOnly`, `runtimeOnly`, `annotationProcessor`, `testImplementation`, `testRuntimeOnly`) and a plain-English explanation of what it provides and why that scope was chosen. Cover: all Spring Boot starters (what each pulls in and what it auto-configures), the Postgres JDBC driver, the Avro/Schema Registry libs and the `exclude` blocks, Lombok (both `compileOnly` and `annotationProcessor` entries and why both are needed), MapStruct (the three entries and why `lombok-mapstruct-binding` is critical for ordering), SpringDoc, and all test dependencies (Testcontainers BOM, each container module, Awaitility, JUnit platform launcher). Also document the Avro plugin config block (`stringType`, `fieldVisibility`, why `sourceSets` must be extended), the test task (`useJUnitPlatform()` and what breaks without it), and a dependency scope quick-reference table and a full logical dependency tree at the end.
>
> Each doc should follow this structure: an overview of the component's role in the system, a Docker Compose snippet with a config table, an application config snippet with a config table, component-specific sections, and a Programming Language Integration section at the end.

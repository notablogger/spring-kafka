# PostgreSQL — Reference Guide

How PostgreSQL is used in this project, explained for future reference.

---

## Role in This Project

PostgreSQL is the **write source of truth**. Every create, update, and delete goes here first. It owns the authoritative state of employees and departments.

```
POST/PUT/DELETE → EmployeeService → PostgreSQL
```

GET requests do NOT read from Postgres — they read from MongoDB (the event log).

---

## Docker Compose Config

```yaml
postgres:
  image: postgres:16
  container_name: training-postgres
  environment:
    POSTGRES_DB: kafka_training_db
    POSTGRES_USER: traininguser
    POSTGRES_PASSWORD: trainingpassword
  ports:
    - "5432:5432"
  volumes:
    - postgres_data:/var/lib/postgresql/data
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U traininguser -d kafka_training_db"]
    interval: 10s
    timeout: 5s
    retries: 5
```

| Config | Value | What it means |
|---|---|---|
| `POSTGRES_DB` | `kafka_training_db` | Database created on first start |
| `POSTGRES_USER` | `traininguser` | Superuser for this database |
| `POSTGRES_PASSWORD` | `trainingpassword` | Password for traininguser |
| `ports: 5432:5432` | `5432` | Standard Postgres port — exposed so you can connect with a DB tool locally |
| `volumes` | `postgres_data` | Data is persisted in a named Docker volume — survives container restarts |
| `healthcheck` | `pg_isready` | Checks the DB is accepting connections before dependent services start |

---

## application.yml — Postgres Config

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/kafka_training_db
    username: traininguser
    password: trainingpassword
    driver-class-name: org.postgresql.Driver
  jpa:
    database-platform: org.hibernate.dialect.PostgreSQLDialect
    hibernate:
      ddl-auto: update
    show-sql: true
    properties:
      hibernate:
        format_sql: true
```

| Config | Value | What it means |
|---|---|---|
| `url` | `jdbc:postgresql://...` | JDBC connection string — host, port, database name |
| `driver-class-name` | `org.postgresql.Driver` | JDBC driver — tells Spring which DB type to use |
| `database-platform` | `PostgreSQLDialect` | Tells Hibernate to generate PostgreSQL-specific SQL |
| `ddl-auto: update` | `update` | Hibernate auto-creates/updates tables on startup from entity classes. **Never use in production — use Flyway/Liquibase instead** |
| `show-sql: true` | `true` | Logs every SQL query — useful for learning, turn off in production |
| `format_sql: true` | `true` | Formats logged SQL to be human-readable |

---

## Schema — What Gets Created

Hibernate reads your `@Entity` classes and creates these tables automatically:

### `departments`
```sql
CREATE TABLE departments (
    id       BIGSERIAL PRIMARY KEY,
    name     VARCHAR NOT NULL UNIQUE,
    location VARCHAR NOT NULL
);
```

### `employees`
```sql
CREATE TABLE employees (
    id            BIGSERIAL PRIMARY KEY,
    first_name    VARCHAR NOT NULL,
    last_name     VARCHAR NOT NULL,
    email         VARCHAR NOT NULL UNIQUE,
    salary        NUMERIC(18,2) NOT NULL,
    hire_date     DATE NOT NULL,
    department_id BIGINT NOT NULL REFERENCES departments(id)
);
```

The `department_id` foreign key enforces that every employee must belong to an existing department.

---

## JPA Entities

### Department.java
```java
@Entity
@Table(name = "departments")
public class Department {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    @Column(nullable = false)
    private String location;

    @OneToMany(mappedBy = "department", fetch = FetchType.LAZY)
    private List<Employee> employees;
}
```

### Employee.java
```java
@Entity
@Table(name = "employees")
public class Employee {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "department_id", nullable = false)
    private Department department;
    // ... other fields
}
```

**`FetchType.LAZY`** — Hibernate does NOT load the related entity until you access it. Requires an open transaction. See the `LazyInitializationException` note below.

---

## Key Concept — LazyInitializationException

A bug this project hit in production. Accessing a lazy collection outside a transaction throws:
```
org.hibernate.LazyInitializationException:
  failed to lazily initialize a collection — no Session
```

**Fix:** Annotate service read methods with `@Transactional(readOnly = true)` to keep the session open for the full method.

```java
@Transactional(readOnly = true)   // ← keeps Hibernate session open
public List<DepartmentResponse> getAll() {
    return departmentRepository.findAll().stream()
            .map(this::toResponse)  // accesses employees.size() — safe now
            .toList();
}
```

`readOnly = true` also tells Hibernate to skip dirty checking and flush — makes reads faster.

---

## Repositories

Spring Data JPA — no SQL needed, method names generate the queries:

```java
// EmployeeRepository
Optional<Employee> findByEmail(String email);
List<Employee> findByDepartmentId(Long departmentId);

// DepartmentRepository
Optional<Department> findByName(String name);
```

Spring translates `findByDepartmentId` into:
```sql
SELECT * FROM employees WHERE department_id = ?
```

---

## In Tests

Testcontainers spins up a real Postgres 16 container — no H2, no in-memory DB:

```java
@Container
static final PostgreSQLContainer<?> postgres =
    new PostgreSQLContainer<>("postgres:16")
        .withDatabaseName("kafka_training_db_test")
        .withUsername("traininguser")
        .withPassword("trainingpassword");
```

`ddl-auto: create-drop` is used in tests — schema is created fresh for each test run and dropped at the end.

---

## Spring Integration

How PostgreSQL and JPA are wired into Spring Boot — every library class, annotation, and what it does.

### Dependencies (`build.gradle`)

```groovy
implementation 'org.springframework.boot:spring-boot-starter-data-jpa'
runtimeOnly 'org.postgresql:postgresql'
compileOnly 'org.projectlombok:lombok'
annotationProcessor 'org.projectlombok:lombok'
```

| Library | Purpose |
|---|---|
| `spring-boot-starter-data-jpa` | Pulls in Hibernate ORM, Spring Data JPA, and auto-configures the `EntityManagerFactory` and transaction manager from `application.yml` |
| `postgresql` | JDBC driver — runtime-only because it's loaded by the JVM at runtime, not needed at compile time |
| `lombok` | Annotation processor that generates boilerplate: getters, setters, constructors, builders, loggers |

---

### Entities

#### `Department`

```java
@Entity
@Table(name = "departments")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class Department {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    @Column(nullable = false)
    private String location;

    @OneToMany(mappedBy = "department", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<Employee> employees;
}
```

#### `Employee`

```java
@Entity
@Table(name = "employees")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class Employee {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "department_id", nullable = false)
    private Department department;

    // ... other fields
}
```

| Class / Annotation | Package | Purpose |
|---|---|---|
| `@Entity` | `jakarta.persistence` | Marks this class as a JPA-managed entity — Hibernate maps it to a database table |
| `@Table(name = ...)` | `jakarta.persistence` | Explicitly sets the table name — without this, Hibernate derives one from the class name |
| `@Id` | `jakarta.persistence` | Marks the primary key field |
| `@GeneratedValue(strategy = IDENTITY)` | `jakarta.persistence` | Delegates ID generation to the database — Postgres uses `BIGSERIAL` (auto-increment) |
| `@Column(nullable = false, unique = true)` | `jakarta.persistence` | Maps to a column with `NOT NULL` and `UNIQUE` constraints in the DDL |
| `@OneToMany(mappedBy = "department")` | `jakarta.persistence` | Declares the one-to-many side of the relationship. `mappedBy` tells Hibernate the FK is owned by `Employee.department`, not here |
| `@ManyToOne(fetch = FetchType.LAZY)` | `jakarta.persistence` | Declares the many-to-one side. `LAZY` means Hibernate does NOT load the department until `.getDepartment()` is called — requires an open session (see `@Transactional` below) |
| `@JoinColumn(name = "department_id")` | `jakarta.persistence` | Maps the FK column in the `employees` table |
| `CascadeType.ALL` | `jakarta.persistence` | Any operation on `Department` (persist, merge, delete) cascades to its `Employee` children |
| `@Getter @Setter` | `lombok` | Generates all getters and setters at compile time |
| `@NoArgsConstructor` | `lombok` | Generates a no-arg constructor — required by JPA/Hibernate for entity instantiation |
| `@AllArgsConstructor` | `lombok` | Generates an all-fields constructor |
| `@Builder` | `lombok` | Generates a fluent builder — used throughout service and test code instead of chained setters |

---

### Repositories

```java
@Repository
public interface EmployeeRepository extends JpaRepository<Employee, Long> {
    Optional<Employee> findByEmail(String email);
    List<Employee> findByDepartmentId(Long departmentId);
}

@Repository
public interface DepartmentRepository extends JpaRepository<Department, Long> {
    Optional<Department> findByName(String name);
}
```

| Class / Annotation | Package | Purpose |
|---|---|---|
| `@Repository` | `org.springframework.stereotype` | Marks this as a Spring data access bean. Also enables translation of JDBC exceptions into Spring's `DataAccessException` hierarchy |
| `JpaRepository<T, ID>` | `org.springframework.data.jpa.repository` | Extends `CrudRepository` and `PagingAndSortingRepository` — provides `findAll()`, `findById()`, `save()`, `deleteById()` and more out of the box |
| `Optional<T>` | `java.util` | Return type for single-result queries — forces callers to handle the not-found case explicitly instead of returning `null` |
| Method name queries (`findByEmail`, `findByDepartmentId`) | Spring Data | Spring Data parses the method name and generates the SQL at startup. `findByDepartmentId` → `SELECT * FROM employees WHERE department_id = ?` |

---

### Service Layer — `@Transactional`

```java
@Service
@RequiredArgsConstructor
public class DepartmentService {

    @Transactional(readOnly = true)
    public List<DepartmentResponse> getAll() {
        return departmentRepository.findAll().stream()
                .map(this::toResponse)  // accesses d.getEmployees().size() — LAZY, needs open session
                .toList();
    }

    @Transactional
    public DepartmentResponse create(DepartmentRequest request) { ... }

    @Transactional
    public void delete(Long id) { ... }
}
```

| Class / Annotation | Package | Purpose |
|---|---|---|
| `@Service` | `org.springframework.stereotype` | Marks this as a Spring service bean — semantically distinct from `@Component`, same behaviour |
| `@RequiredArgsConstructor` | `lombok` | Constructor injection for all `final` fields — the Spring-recommended way to inject dependencies |
| `@Transactional` | `org.springframework.transaction.annotation` | Wraps the method in a database transaction. On write methods: starts a transaction, commits on return, rolls back on exception |
| `@Transactional(readOnly = true)` | `org.springframework.transaction.annotation` | Same as above but tells Hibernate to skip dirty checking and flush — faster reads. Also keeps the Hibernate session open for the full method, which is **required** for accessing `LAZY` collections like `department.getEmployees()` |

**Why `readOnly = true` matters here:** `Department.employees` is `FetchType.LAZY`. Without `@Transactional`, the Hibernate session closes after `findAll()` returns, and calling `.getEmployees().size()` inside `toResponse()` throws `LazyInitializationException`. The `@Transactional(readOnly = true)` keeps the session alive for the duration of the method.

---

### Error Handling — `GlobalExceptionHandler`

```java
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNotFound(ResourceNotFoundException ex) {
        return buildResponse(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {
        List<String> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .toList();
        // ...
    }
}
```

| Class / Annotation | Package | Purpose |
|---|---|---|
| `@RestControllerAdvice` | `org.springframework.web.bind.annotation` | Applies `@ExceptionHandler` methods globally to all `@RestController` classes — one place for all error responses instead of try/catch in every controller |
| `@ExceptionHandler(X.class)` | `org.springframework.web.bind.annotation` | Registers this method as the handler for exceptions of type `X` thrown anywhere in a controller |
| `ResourceNotFoundException` | project | Custom `RuntimeException` subclass — thrown by service layer when an entity is not found. `RuntimeException` means Spring doesn't require it to be declared with `throws` |
| `MethodArgumentNotValidException` | `org.springframework.web.bind` | Thrown automatically by Spring when `@Valid` fails on a request body — contains all field-level validation errors |
| `ResponseEntity<T>` | `org.springframework.http` | Wrapper for the HTTP response — lets you set the status code, headers, and body explicitly |
| `HttpStatus` | `org.springframework.http` | Enum of HTTP status codes — `NOT_FOUND (404)`, `BAD_REQUEST (400)`, `INTERNAL_SERVER_ERROR (500)` |
| `BindingResult.getFieldErrors()` | `org.springframework.validation` | Returns a list of `FieldError` objects — each has the field name and the violated constraint message |

---

### DTOs and Validation

```java
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
@Schema(description = "Payload for creating or updating an employee")
public class EmployeeRequest {

    @NotBlank(message = "First name is required")
    private String firstName;

    @Email(message = "Email must be valid")
    private String email;

    @DecimalMin(value = "0.0", inclusive = false, message = "Salary must be greater than 0")
    private BigDecimal salary;

    @NotNull(message = "Department ID is required")
    private Long departmentId;
}
```

| Class / Annotation | Package | Purpose |
|---|---|---|
| `@NotBlank` | `jakarta.validation.constraints` | Fails if the string is null, empty, or whitespace-only |
| `@NotNull` | `jakarta.validation.constraints` | Fails if the value is null — used for non-string types like `Long`, `BigDecimal` |
| `@Email` | `jakarta.validation.constraints` | Validates the string is a well-formed email address |
| `@DecimalMin` | `jakarta.validation.constraints` | Validates the value is above the minimum. `inclusive = false` means strictly greater than, not equal to |
| `@Valid` | `jakarta.validation` | Placed on the `@RequestBody` parameter in controllers — triggers Spring to run all constraint annotations on the object before the method is called |
| `@Schema` | `io.swagger.v3.oas.annotations.media` | Adds description and example metadata to the Swagger UI — has no runtime effect on behaviour |

---

### Controllers

```java
@RestController
@RequestMapping("/api/employees")
@RequiredArgsConstructor
@Tag(name = "Employees", description = "CRUD operations for employees")
public class EmployeeController {

    @PostMapping
    public ResponseEntity<EmployeeResponse> create(@Valid @RequestBody EmployeeRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(employeeService.create(request));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        employeeService.delete(id);
        return ResponseEntity.noContent().build();
    }
}
```

| Class / Annotation | Package | Purpose |
|---|---|---|
| `@RestController` | `org.springframework.web.bind.annotation` | Combines `@Controller` + `@ResponseBody` — every method returns data serialised to JSON, not a view name |
| `@RequestMapping` | `org.springframework.web.bind.annotation` | Sets the base URL path for all methods in this controller |
| `@GetMapping / @PostMapping / @PutMapping / @DeleteMapping` | `org.springframework.web.bind.annotation` | HTTP verb-specific shorthand for `@RequestMapping(method = ...)` |
| `@PathVariable` | `org.springframework.web.bind.annotation` | Extracts a variable from the URL path — `/{id}` → `Long id` |
| `@RequestBody` | `org.springframework.web.bind.annotation` | Deserialises the HTTP request body JSON into the annotated parameter |
| `@Valid` | `jakarta.validation` | Triggers constraint validation on the request body before the method executes |
| `ResponseEntity<T>` | `org.springframework.http` | Lets you control the exact status code. `ResponseEntity.status(CREATED).body(...)` → HTTP 201. `ResponseEntity.noContent().build()` → HTTP 204 |
| `@Tag` | `io.swagger.v3.oas.annotations.tags` | Groups this controller's endpoints under a named section in Swagger UI |
| `@Operation` | `io.swagger.v3.oas.annotations` | Adds a summary and description to an individual endpoint in Swagger UI |
| `@ApiResponse` | `io.swagger.v3.oas.annotations.responses` | Documents a specific HTTP response code and description in Swagger UI |
| `@Parameter` | `io.swagger.v3.oas.annotations` | Adds description metadata to a path variable or query parameter in Swagger UI |

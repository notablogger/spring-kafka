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


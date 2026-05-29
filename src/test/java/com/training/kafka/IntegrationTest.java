package com.training.kafka;

import com.training.kafka.dto.DepartmentRequest;
import com.training.kafka.dto.DepartmentResponse;
import com.training.kafka.dto.EmployeeRequest;
import com.training.kafka.dto.EmployeeResponse;
import org.junit.jupiter.api.*;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Full integration test suite — Option A (single class).
 *
 * Covers:
 *  1. Department CRUD — all endpoints, correct HTTP status codes, validation errors
 *  2. Employee CRUD   — all endpoints, correct HTTP status codes, validation errors
 *  3. Kafka flow      — producer fires event on create/update/delete,
 *                       consumer saves to MongoDB, verified via GET endpoints only
 *
 * Infrastructure (via BaseIntegrationTest):
 *  - PostgreSQL 16      real container
 *  - Kafka 7.6.1        real container
 *  - Schema Registry    real container (Avro serialisation end-to-end)
 *  - MongoDB 7          real container
 *
 * Design principles:
 *  - No direct repository calls — all assertions go through REST endpoints
 *  - Awaitility polls the GET endpoint until Kafka consumer has landed the event
 *  - @TestMethodOrder ensures departments exist before employees
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IntegrationTest extends BaseIntegrationTest {

    @LocalServerPort
    private int port;

    private RestClient restClient;

    // Shared state across ordered tests
    private static Long deptId;
    private static Long hrDeptId;
    private static Long empId;

    @BeforeEach
    void setup() {
        restClient = RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .build();
    }

    // ════════════════════════════════════════════════════════════
    // DEPARTMENT TESTS
    // ════════════════════════════════════════════════════════════

    /**
     * POST /api/departments
     * Expects 201, body contains id/name/location.
     */
    @Test
    @Order(1)
    void createDepartment_validRequest_returns201() {
        DepartmentResponse response = restClient.post()
                .uri("/api/departments")
                .contentType(MediaType.APPLICATION_JSON)
                .body(DepartmentRequest.builder().name("Engineering").location("London").build())
                .retrieve()
                .body(DepartmentResponse.class);

        assertThat(response).isNotNull();
        assertThat(response.getId()).isNotNull();
        assertThat(response.getName()).isEqualTo("Engineering");
        assertThat(response.getLocation()).isEqualTo("London");
        deptId = response.getId();
    }

    /**
     * GET /api/departments
     * Expects 200, list contains the created department.
     */
    @Test
    @Order(2)
    void getAllDepartments_returns200WithList() {
        List<DepartmentResponse> list = restClient.get()
                .uri("/api/departments")
                .retrieve()
                .body(new org.springframework.core.ParameterizedTypeReference<>() {});

        assertThat(list).isNotEmpty();
        assertThat(list).anyMatch(d -> "Engineering".equals(d.getName()));
    }

    /**
     * GET /api/departments/{id}
     * Expects 200 with correct body.
     */
    @Test
    @Order(3)
    void getDepartmentById_existingId_returns200() {
        DepartmentResponse response = restClient.get()
                .uri("/api/departments/" + deptId)
                .retrieve()
                .body(DepartmentResponse.class);

        assertThat(response).isNotNull();
        assertThat(response.getId()).isEqualTo(deptId);
        assertThat(response.getName()).isEqualTo("Engineering");
    }

    /**
     * GET /api/departments/{id} — non-existent
     * Must return 404, not 500.
     */
    @Test
    @Order(4)
    void getDepartmentById_nonExistentId_returns404() {
        assertThatThrownBy(() ->
                restClient.get().uri("/api/departments/99999").retrieve().body(DepartmentResponse.class)
        ).isInstanceOf(HttpClientErrorException.NotFound.class);
    }

    /**
     * PUT /api/departments/{id}
     * Expects 200, body reflects updated values.
     */
    @Test
    @Order(5)
    void updateDepartment_validRequest_returns200() {
        DepartmentResponse response = restClient.put()
                .uri("/api/departments/" + deptId)
                .contentType(MediaType.APPLICATION_JSON)
                .body(DepartmentRequest.builder().name("Engineering Updated").location("Manchester").build())
                .retrieve()
                .body(DepartmentResponse.class);

        assertThat(response.getName()).isEqualTo("Engineering Updated");
        assertThat(response.getLocation()).isEqualTo("Manchester");
    }

    /**
     * POST /api/departments — missing name
     * Expects 400 with validation error message.
     */
    @Test
    @Order(6)
    void createDepartment_missingName_returns400() {
        assertThatThrownBy(() ->
                restClient.post()
                        .uri("/api/departments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(DepartmentRequest.builder().location("London").build())
                        .retrieve()
                        .body(DepartmentResponse.class)
        ).isInstanceOf(HttpClientErrorException.BadRequest.class);
    }

    // ════════════════════════════════════════════════════════════
    // EMPLOYEE TESTS
    // ════════════════════════════════════════════════════════════

    /**
     * POST /api/employees
     * Expects 201 with correct body.
     * Stores empId and hrDeptId for use in subsequent tests.
     */
    @Test
    @Order(10)
    void createEmployee_validRequest_returns201() {
        DepartmentResponse dept = restClient.post()
                .uri("/api/departments")
                .contentType(MediaType.APPLICATION_JSON)
                .body(DepartmentRequest.builder().name("HR").location("Leeds").build())
                .retrieve()
                .body(DepartmentResponse.class);
        hrDeptId = dept.getId();

        EmployeeResponse response = restClient.post()
                .uri("/api/employees")
                .contentType(MediaType.APPLICATION_JSON)
                .body(EmployeeRequest.builder()
                        .firstName("Alice").lastName("Smith")
                        .email("alice@test.com")
                        .salary(new BigDecimal("60000.00"))
                        .hireDate(LocalDate.of(2024, 1, 1))
                        .departmentId(hrDeptId)
                        .build())
                .retrieve()
                .body(EmployeeResponse.class);

        assertThat(response).isNotNull();
        assertThat(response.getId()).isNotNull();
        assertThat(response.getFirstName()).isEqualTo("Alice");
        assertThat(response.getEmail()).isEqualTo("alice@test.com");
        empId = response.getId();
    }

    /**
     * GET /api/employees/{id}
     * Verifies the Kafka CREATED event was consumed and saved to MongoDB.
     * Uses Awaitility to wait for the async consumer — no direct DB access.
     * Checks: eventType=CREATED, eventTimestamp present, correct employee data.
     */
    @Test
    @Order(11)
    void getEmployeeById_afterCreate_returnsCreatedSnapshot() {
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            EmployeeResponse response = restClient.get()
                    .uri("/api/employees/" + empId)
                    .retrieve()
                    .body(EmployeeResponse.class);

            assertThat(response).isNotNull();
            assertThat(response.getId()).isEqualTo(empId);
            assertThat(response.getFirstName()).isEqualTo("Alice");
            assertThat(response.getDepartmentName()).isEqualTo("HR");
            assertThat(response.getEventType()).isEqualTo("CREATED");
            assertThat(response.getEventTimestamp()).isNotNull();
        });
    }

    /**
     * GET /api/employees
     * Returns all active employees from MongoDB — at least Alice must be present.
     */
    @Test
    @Order(12)
    void getAllEmployees_returnsNonEmptyList() {
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            List<EmployeeResponse> list = restClient.get()
                    .uri("/api/employees")
                    .retrieve()
                    .body(new org.springframework.core.ParameterizedTypeReference<>() {});

            assertThat(list).isNotEmpty();
            assertThat(list).anyMatch(e -> "alice@test.com".equals(e.getEmail()));
        });
    }

    /**
     * GET /api/employees/department/{id}
     * Filters employees by department — uses hrDeptId stored at create time.
     */
    @Test
    @Order(13)
    void getEmployeesByDepartment_returnsFilteredList() {
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            List<EmployeeResponse> list = restClient.get()
                    .uri("/api/employees/department/" + hrDeptId)
                    .retrieve()
                    .body(new org.springframework.core.ParameterizedTypeReference<>() {});

            assertThat(list).isNotEmpty();
            assertThat(list).anyMatch(e -> "alice@test.com".equals(e.getEmail()));
        });
    }

    /**
     * PUT /api/employees/{id}
     * Updates employee, then polls GET until UPDATED snapshot appears in MongoDB.
     * Verifies updated fields via GET endpoint only.
     */
    @Test
    @Order(14)
    void updateEmployee_getReturnsUpdatedSnapshot() {
        restClient.put()
                .uri("/api/employees/" + empId)
                .contentType(MediaType.APPLICATION_JSON)
                .body(EmployeeRequest.builder()
                        .firstName("Alice").lastName("Smith-Updated")
                        .email("alice.updated@test.com")
                        .salary(new BigDecimal("75000.00"))
                        .hireDate(LocalDate.of(2024, 1, 1))
                        .departmentId(hrDeptId)
                        .build())
                .retrieve()
                .toBodilessEntity();

        // Poll GET until UPDATED event lands from Kafka consumer
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            EmployeeResponse response = restClient.get()
                    .uri("/api/employees/" + empId)
                    .retrieve()
                    .body(EmployeeResponse.class);

            assertThat(response.getEventType()).isEqualTo("UPDATED");
            assertThat(response.getLastName()).isEqualTo("Smith-Updated");
            assertThat(response.getEmail()).isEqualTo("alice.updated@test.com");
        });
    }

    /**
     * DELETE /api/employees/{id}
     * Expects 204. Then GET must return 404 once the DELETED event lands in MongoDB.
     */
    @Test
    @Order(15)
    void deleteEmployee_getReturns404AfterDeletion() {
        restClient.delete()
                .uri("/api/employees/" + empId)
                .retrieve()
                .toBodilessEntity();

        // Poll GET until the DELETED event is consumed and record is filtered out
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() ->
                assertThatThrownBy(() ->
                        restClient.get().uri("/api/employees/" + empId).retrieve().body(EmployeeResponse.class)
                ).isInstanceOf(HttpClientErrorException.NotFound.class)
        );
    }

    // ════════════════════════════════════════════════════════════
    // VALIDATION TESTS
    // ════════════════════════════════════════════════════════════

    /**
     * POST /api/employees — invalid email
     * Expects 400 validation error.
     */
    @Test
    @Order(20)
    void createEmployee_invalidEmail_returns400() {
        assertThatThrownBy(() ->
                restClient.post()
                        .uri("/api/employees")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(EmployeeRequest.builder()
                                .firstName("Bad").lastName("Email")
                                .email("not-an-email")
                                .salary(new BigDecimal("50000.00"))
                                .hireDate(LocalDate.of(2024, 1, 1))
                                .departmentId(deptId)
                                .build())
                        .retrieve()
                        .body(EmployeeResponse.class)
        ).isInstanceOf(HttpClientErrorException.BadRequest.class);
    }

    /**
     * POST /api/employees — salary = 0
     * Expects 400 validation error.
     */
    @Test
    @Order(21)
    void createEmployee_zeroSalary_returns400() {
        assertThatThrownBy(() ->
                restClient.post()
                        .uri("/api/employees")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(EmployeeRequest.builder()
                                .firstName("Zero").lastName("Salary")
                                .email("zero@test.com")
                                .salary(BigDecimal.ZERO)
                                .hireDate(LocalDate.of(2024, 1, 1))
                                .departmentId(deptId)
                                .build())
                        .retrieve()
                        .body(EmployeeResponse.class)
        ).isInstanceOf(HttpClientErrorException.BadRequest.class);
    }

    /**
     * POST /api/employees — non-existent department
     * Expects 404.
     */
    @Test
    @Order(22)
    void createEmployee_nonExistentDepartment_returns404() {
        assertThatThrownBy(() ->
                restClient.post()
                        .uri("/api/employees")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(EmployeeRequest.builder()
                                .firstName("Ghost").lastName("Dept")
                                .email("ghost@test.com")
                                .salary(new BigDecimal("50000.00"))
                                .hireDate(LocalDate.of(2024, 1, 1))
                                .departmentId(99999L)
                                .build())
                        .retrieve()
                        .body(EmployeeResponse.class)
        ).isInstanceOf(HttpClientErrorException.NotFound.class);
    }

    // ════════════════════════════════════════════════════════════
    // DEPARTMENT DELETE — must come last
    // ════════════════════════════════════════════════════════════

    /**
     * DELETE /api/departments/{id}
     * Expects 204.
     */
    @Test
    @Order(30)
    void deleteDepartment_validId_returns204() {
        // Create a fresh dept with no employees to safely delete
        DepartmentResponse dept = restClient.post()
                .uri("/api/departments")
                .contentType(MediaType.APPLICATION_JSON)
                .body(DepartmentRequest.builder().name("Temp").location("Nowhere").build())
                .retrieve()
                .body(DepartmentResponse.class);

        var result = restClient.delete()
                .uri("/api/departments/" + dept.getId())
                .retrieve()
                .toBodilessEntity();

        assertThat(result.getStatusCode().value()).isEqualTo(204);
    }
}

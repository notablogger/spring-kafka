package com.nik.kafka.kafka;

import com.nik.kafka.BaseIntegrationTest;
import com.nik.kafka.dto.DepartmentRequest;
import com.nik.kafka.dto.DepartmentResponse;
import com.nik.kafka.dto.EmployeeRequest;
import com.nik.kafka.dto.EmployeeResponse;
import com.nik.kafka.entity.Employee;
import com.nik.kafka.repository.EmployeeRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end test:
 *  1. Calls real REST endpoint  → EmployeeService
 *  2. EmployeeService           → EmployeeEventProducer sends to Kafka
 *  3. EmployeeEventConsumer     → consumes event, maps via MapStruct, updates DB
 *  4. Assert                    → DB reflects the change
 */
class EmployeeEndToEndTest extends BaseIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private EmployeeRepository employeeRepository;

    private RestClient restClient;

    @BeforeEach
    void setup() {
        restClient = RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .build();
    }

    // ─────────────────────────────────────────────────────────────
    // CREATE: POST /api/employees → producer sends CREATED event
    //         consumer receives it → sets lastUpdated in DB
    // ─────────────────────────────────────────────────────────────
    @Test
    void createEmployee_producerSendsEvent_consumerUpdatesDB() {
        // 1. Create department via REST
        DepartmentResponse dept = restClient.post()
                .uri("/api/departments")
                .contentType(MediaType.APPLICATION_JSON)
                .body(DepartmentRequest.builder().name("Engineering").location("New York").build())
                .retrieve()
                .body(DepartmentResponse.class);

        assertThat(dept).isNotNull();
        assertThat(dept.getId()).isNotNull();

        // 2. Create employee via REST → triggers EmployeeEventProducer.sendEmployeeCreatedEvent()
        EmployeeResponse created = restClient.post()
                .uri("/api/employees")
                .contentType(MediaType.APPLICATION_JSON)
                .body(EmployeeRequest.builder()
                        .firstName("John")
                        .lastName("Doe")
                        .email("john.doe@test.com")
                        .salary(new BigDecimal("75000.00"))
                        .hireDate(LocalDate.of(2024, 1, 15))
                        .departmentId(dept.getId())
                        .build())
                .retrieve()
                .body(EmployeeResponse.class);

        assertThat(created).isNotNull();
        Long employeeId = created.getId();

        // 3. Wait for EmployeeEventConsumer to consume CREATED event and set lastUpdated
        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            Employee employee = employeeRepository.findById(employeeId).orElseThrow();
            assertThat(employee.getLastUpdated())
                    .as("Consumer should have set lastUpdated after consuming CREATED event")
                    .isNotNull();
            assertThat(employee.getFirstName()).isEqualTo("John");
            assertThat(employee.getLastName()).isEqualTo("Doe");
        });
    }

    // ─────────────────────────────────────────────────────────────
    // UPDATE: PUT /api/employees/{id} → producer sends UPDATED event
    //         consumer receives it → updates all fields + lastUpdated in DB
    // ─────────────────────────────────────────────────────────────
    @Test
    void updateEmployee_producerSendsEvent_consumerUpdatesAllFieldsInDB() {
        // 1. Setup
        DepartmentResponse dept = restClient.post()
                .uri("/api/departments")
                .contentType(MediaType.APPLICATION_JSON)
                .body(DepartmentRequest.builder().name("HR").location("Chicago").build())
                .retrieve()
                .body(DepartmentResponse.class);

        EmployeeResponse created = restClient.post()
                .uri("/api/employees")
                .contentType(MediaType.APPLICATION_JSON)
                .body(EmployeeRequest.builder()
                        .firstName("Jane")
                        .lastName("Smith")
                        .email("jane.smith@test.com")
                        .salary(new BigDecimal("80000.00"))
                        .hireDate(LocalDate.of(2023, 6, 1))
                        .departmentId(dept.getId())
                        .build())
                .retrieve()
                .body(EmployeeResponse.class);

        // wait for CREATED event to be consumed first
        await().atMost(20, TimeUnit.SECONDS).until(() ->
                employeeRepository.findById(created.getId())
                        .map(e -> e.getLastUpdated() != null)
                        .orElse(false));

        // 2. Update via REST → triggers EmployeeEventProducer.sendEmployeeUpdatedEvent()
        restClient.put()
                .uri("/api/employees/" + created.getId())
                .contentType(MediaType.APPLICATION_JSON)
                .body(EmployeeRequest.builder()
                        .firstName("Jane")
                        .lastName("Smith-Updated")
                        .email("jane.smith.updated@test.com")
                        .salary(new BigDecimal("95000.00"))
                        .hireDate(LocalDate.of(2023, 6, 1))
                        .departmentId(dept.getId())
                        .build())
                .retrieve()
                .toBodilessEntity();

        // 3. Wait for EmployeeEventConsumer to consume UPDATED event and update DB
        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            Employee employee = employeeRepository.findById(created.getId()).orElseThrow();
            assertThat(employee.getLastName())
                    .as("Consumer should have updated lastName from Kafka UPDATED event")
                    .isEqualTo("Smith-Updated");
            assertThat(employee.getEmail()).isEqualTo("jane.smith.updated@test.com");
            assertThat(employee.getSalary()).isEqualByComparingTo(new BigDecimal("95000.00"));
            assertThat(employee.getLastUpdated()).isNotNull();
        });
    }

    // ─────────────────────────────────────────────────────────────
    // DELETE: DELETE /api/employees/{id} → producer sends DELETED event
    //         consumer receives it → employee removed from DB
    // ─────────────────────────────────────────────────────────────
    @Test
    void deleteEmployee_endpointReturns204_employeeRemovedFromDB() {
        DepartmentResponse dept = restClient.post()
                .uri("/api/departments")
                .contentType(MediaType.APPLICATION_JSON)
                .body(DepartmentRequest.builder().name("Finance").location("Boston").build())
                .retrieve()
                .body(DepartmentResponse.class);

        EmployeeResponse created = restClient.post()
                .uri("/api/employees")
                .contentType(MediaType.APPLICATION_JSON)
                .body(EmployeeRequest.builder()
                        .firstName("Bob")
                        .lastName("Builder")
                        .email("bob.builder@test.com")
                        .salary(new BigDecimal("60000.00"))
                        .hireDate(LocalDate.of(2021, 9, 20))
                        .departmentId(dept.getId())
                        .build())
                .retrieve()
                .body(EmployeeResponse.class);

        // 2. Delete via REST
        restClient.delete()
                .uri("/api/employees/" + created.getId())
                .retrieve()
                .toBodilessEntity();

        // 3. Employee is deleted from DB by the service directly
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(employeeRepository.findById( created.getId())).isEmpty());
    }
}


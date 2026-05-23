package com.nik.kafka.service;

import com.nik.kafka.dto.EmployeeRequest;
import com.nik.kafka.dto.EmployeeResponse;
import com.nik.kafka.entity.Department;
import com.nik.kafka.entity.Employee;
import com.nik.kafka.entity.EmployeeEventDocument;
import com.nik.kafka.kafka.EmployeeEventProducer;
import com.nik.kafka.repository.DepartmentRepository;
import com.nik.kafka.repository.EmployeeEventDocumentRepository;
import com.nik.kafka.repository.EmployeeRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class EmployeeService {

    private final EmployeeRepository employeeRepository;
    private final DepartmentRepository departmentRepository;
    private final EmployeeEventProducer employeeEventProducer;
    private final EmployeeEventDocumentRepository eventDocumentRepository;

    // ─── READ from MongoDB ────────────────────────────────────────

    public List<EmployeeResponse> getAll() {
        // Get the latest event per employee; exclude DELETED
        return eventDocumentRepository.findAll().stream()
                .collect(java.util.stream.Collectors.toMap(
                        EmployeeEventDocument::getEmployeeId,
                        d -> d,
                        (a, b) -> a.getEventTimestamp().isAfter(b.getEventTimestamp()) ? a : b
                ))
                .values().stream()
                .filter(d -> !"DELETED".equals(d.getEventType()))
                .map(this::fromDocument)
                .toList();
    }

    public EmployeeResponse getById(Long id) {
        EmployeeEventDocument doc = eventDocumentRepository
                .findTopByEmployeeIdOrderByEventTimestampDesc(id)
                .orElseThrow(() -> new RuntimeException("Employee not found with id: " + id));
        if ("DELETED".equals(doc.getEventType())) {
            throw new RuntimeException("Employee not found with id: " + id);
        }
        return fromDocument(doc);
    }

    public List<EmployeeResponse> getByDepartment(Long departmentId) {
        Department dept = findDepartmentOrThrow(departmentId);
        return eventDocumentRepository.findAll().stream()
                .collect(java.util.stream.Collectors.toMap(
                        EmployeeEventDocument::getEmployeeId,
                        d -> d,
                        (a, b) -> a.getEventTimestamp().isAfter(b.getEventTimestamp()) ? a : b
                ))
                .values().stream()
                .filter(d -> !"DELETED".equals(d.getEventType()))
                .filter(d -> dept.getName().equals(d.getDepartmentName()))
                .map(this::fromDocument)
                .toList();
    }

    // ─── WRITE to Postgres + Kafka ────────────────────────────────

    @Transactional
    public EmployeeResponse create(EmployeeRequest request) {
        Department department = findDepartmentOrThrow(request.getDepartmentId());
        Employee employee = Employee.builder()
                .firstName(request.getFirstName())
                .lastName(request.getLastName())
                .email(request.getEmail())
                .salary(request.getSalary())
                .hireDate(request.getHireDate())
                .department(department)
                .build();
        Employee saved = employeeRepository.save(employee);
        employeeEventProducer.sendEmployeeCreatedEvent(saved);
        return toResponse(saved);
    }

    @Transactional
    public EmployeeResponse update(Long id, EmployeeRequest request) {
        Employee employee = findOrThrow(id);
        Department department = findDepartmentOrThrow(request.getDepartmentId());
        employee.setFirstName(request.getFirstName());
        employee.setLastName(request.getLastName());
        employee.setEmail(request.getEmail());
        employee.setSalary(request.getSalary());
        employee.setHireDate(request.getHireDate());
        employee.setDepartment(department);
        Employee saved = employeeRepository.save(employee);
        employeeEventProducer.sendEmployeeUpdatedEvent(saved);
        return toResponse(saved);
    }

    @Transactional
    public void delete(Long id) {
        Employee employee = findOrThrow(id);
        employeeRepository.deleteById(id);
        employeeEventProducer.sendEmployeeDeletedEvent(employee);
    }

    // ─── Helpers ──────────────────────────────────────────────────

    private Employee findOrThrow(Long id) {
        return employeeRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Employee not found with id: " + id));
    }

    private Department findDepartmentOrThrow(Long id) {
        return departmentRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Department not found with id: " + id));
    }

    /** Map MongoDB document → EmployeeResponse */
    private EmployeeResponse fromDocument(EmployeeEventDocument doc) {
        return EmployeeResponse.builder()
                .id(doc.getEmployeeId())
                .firstName(doc.getFirstName())
                .lastName(doc.getLastName())
                .email(doc.getEmail())
                .salary(doc.getSalary())
                .hireDate(doc.getHireDate())
                .departmentName(doc.getDepartmentName())
                .departmentLocation(doc.getDepartmentLocation())
                .eventType(doc.getEventType())
                .eventTimestamp(doc.getEventTimestamp())
                .build();
    }

    /** Map Postgres entity → EmployeeResponse (used immediately after write, before Kafka event lands) */
    private EmployeeResponse toResponse(Employee e) {
        return EmployeeResponse.builder()
                .id(e.getId())
                .firstName(e.getFirstName())
                .lastName(e.getLastName())
                .email(e.getEmail())
                .salary(e.getSalary())
                .hireDate(e.getHireDate())
                .departmentId(e.getDepartment().getId())
                .departmentName(e.getDepartment().getName())
                .departmentLocation(e.getDepartment().getLocation())
                .build();
    }
}

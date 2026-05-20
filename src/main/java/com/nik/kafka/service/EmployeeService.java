package com.nik.kafka.service;

import com.nik.kafka.dto.EmployeeRequest;
import com.nik.kafka.dto.EmployeeResponse;
import com.nik.kafka.entity.Department;
import com.nik.kafka.entity.Employee;
import com.nik.kafka.kafka.EmployeeEventProducer;
import com.nik.kafka.repository.DepartmentRepository;
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

    public List<EmployeeResponse> getAll() {
        return employeeRepository.findAll().stream()
                .map(this::toResponse)
                .toList();
    }

    public EmployeeResponse getById(Long id) {
        return toResponse(findOrThrow(id));
    }

    public List<EmployeeResponse> getByDepartment(Long departmentId) {
        return employeeRepository.findByDepartmentId(departmentId).stream()
                .map(this::toResponse)
                .toList();
    }

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

    private Employee findOrThrow(Long id) {
        return employeeRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Employee not found with id: " + id));
    }

    private Department findDepartmentOrThrow(Long id) {
        return departmentRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Department not found with id: " + id));
    }

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
                .build();
    }
}

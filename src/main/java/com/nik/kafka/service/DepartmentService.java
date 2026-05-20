package com.nik.kafka.service;

import com.nik.kafka.dto.DepartmentRequest;
import com.nik.kafka.dto.DepartmentResponse;
import com.nik.kafka.entity.Department;
import com.nik.kafka.repository.DepartmentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class DepartmentService {

    private final DepartmentRepository departmentRepository;

    public List<DepartmentResponse> getAll() {
        return departmentRepository.findAll().stream()
                .map(this::toResponse)
                .toList();
    }

    public DepartmentResponse getById(Long id) {
        return toResponse(findOrThrow(id));
    }

    @Transactional
    public DepartmentResponse create(DepartmentRequest request) {
        Department department = Department.builder()
                .name(request.getName())
                .location(request.getLocation())
                .build();
        return toResponse(departmentRepository.save(department));
    }

    @Transactional
    public DepartmentResponse update(Long id, DepartmentRequest request) {
        Department department = findOrThrow(id);
        department.setName(request.getName());
        department.setLocation(request.getLocation());
        return toResponse(departmentRepository.save(department));
    }

    @Transactional
    public void delete(Long id) {
        findOrThrow(id);
        departmentRepository.deleteById(id);
    }

    private Department findOrThrow(Long id) {
        return departmentRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Department not found with id: " + id));
    }

    private DepartmentResponse toResponse(Department d) {
        return DepartmentResponse.builder()
                .id(d.getId())
                .name(d.getName())
                .location(d.getLocation())
                .employeeCount(d.getEmployees() == null ? 0 : d.getEmployees().size())
                .build();
    }
}


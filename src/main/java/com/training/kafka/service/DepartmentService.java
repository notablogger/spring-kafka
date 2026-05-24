package com.training.kafka.service;

import com.training.kafka.dto.DepartmentRequest;
import com.training.kafka.dto.DepartmentResponse;
import com.training.kafka.entity.Department;
import com.training.kafka.exception.ResourceNotFoundException;
import com.training.kafka.repository.DepartmentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class DepartmentService {

    private final DepartmentRepository departmentRepository;

    @Transactional(readOnly = true)
    public List<DepartmentResponse> getAll() {
        return departmentRepository.findAll().stream()
                .map(this::toResponse)
                .toList();
    }

    @Transactional(readOnly = true)
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
                .orElseThrow(() -> new ResourceNotFoundException("Department not found with id: " + id));
    }

    private DepartmentResponse toResponse(Department d) {
        // employees list is LAZY — size() is safe here because we are inside a transaction
        int count = d.getEmployees() == null ? 0 : d.getEmployees().size();
        return DepartmentResponse.builder()
                .id(d.getId())
                .name(d.getName())
                .location(d.getLocation())
                .employeeCount(count)
                .build();
    }
}

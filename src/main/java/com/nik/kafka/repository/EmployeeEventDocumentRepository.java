package com.nik.kafka.repository;

import com.nik.kafka.entity.EmployeeEventDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;
import java.util.Optional;

public interface EmployeeEventDocumentRepository extends MongoRepository<EmployeeEventDocument, String> {
    List<EmployeeEventDocument> findByEmployeeId(Long employeeId);
    Optional<EmployeeEventDocument> findTopByEmployeeIdOrderByEventTimestampDesc(Long employeeId);
}

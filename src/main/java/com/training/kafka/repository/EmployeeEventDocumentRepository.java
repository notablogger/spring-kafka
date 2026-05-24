package com.training.kafka.repository;

import com.training.kafka.entity.EmployeeEventDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;
import java.util.Optional;

public interface EmployeeEventDocumentRepository extends MongoRepository<EmployeeEventDocument, String> {
    List<EmployeeEventDocument> findByEmployeeId(Long employeeId);
    Optional<EmployeeEventDocument> findTopByEmployeeIdOrderByEventTimestampDesc(Long employeeId);
}

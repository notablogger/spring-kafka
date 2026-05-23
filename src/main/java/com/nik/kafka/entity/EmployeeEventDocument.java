package com.nik.kafka.entity;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

@Document(collection = "employee_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EmployeeEventDocument {

    @Id
    private String id;

    private Long employeeId;
    private String firstName;
    private String lastName;
    private String email;
    private BigDecimal salary;
    private LocalDate hireDate;
    private String departmentName;
    private String departmentLocation;
    private String eventType;
    private Instant eventTimestamp;
    private Instant receivedAt;
}


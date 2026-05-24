package com.training.kafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Schema(description = "Employee response payload")
public class EmployeeResponse {

    @Schema(description = "Employee ID", example = "1")
    private Long id;

    @Schema(description = "First name", example = "John")
    private String firstName;

    @Schema(description = "Last name", example = "Doe")
    private String lastName;

    @Schema(description = "Email address", example = "john.doe@company.com")
    private String email;

    @Schema(description = "Salary", example = "75000.00")
    private BigDecimal salary;

    @Schema(description = "Hire date", example = "2024-01-15")
    private LocalDate hireDate;

    @Schema(description = "ID of the department", example = "1")
    private Long departmentId;

    @Schema(description = "Name of the department", example = "Engineering")
    private String departmentName;

    @Schema(description = "Department location", example = "New York")
    private String departmentLocation;

    @Schema(description = "Last event type", example = "CREATED")
    private String eventType;

    @Schema(description = "Timestamp of the last event")
    private Instant eventTimestamp;
}

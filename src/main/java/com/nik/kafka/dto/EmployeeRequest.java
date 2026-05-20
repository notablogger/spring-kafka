package com.nik.kafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDate;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Schema(description = "Payload for creating or updating an employee")
public class EmployeeRequest {

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

    @Schema(description = "ID of the department the employee belongs to", example = "1")
    private Long departmentId;
}

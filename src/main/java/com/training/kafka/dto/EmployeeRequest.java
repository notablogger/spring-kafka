package com.training.kafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDate;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Schema(description = "Payload for creating or updating an employee")
public class  EmployeeRequest {

    @NotBlank(message = "First name is required")
    @Schema(description = "First name", example = "John")
    private String firstName;

    @NotBlank(message = "Last name is required")
    @Schema(description = "Last name", example = "Doe")
    private String lastName;

    @NotBlank(message = "Email is required")
    @Email(message = "Email must be valid")
    @Schema(description = "Email address", example = "john.doe@company.com")
    private String email;

    @NotNull(message = "Salary is required")
    @DecimalMin(value = "0.0", inclusive = false, message = "Salary must be greater than 0")
    @Schema(description = "Salary", example = "75000.00")
    private BigDecimal salary;

    @NotNull(message = "Hire date is required")
    @Schema(description = "Hire date", example = "2024-01-15")
    private LocalDate hireDate;

    @NotNull(message = "Department ID is required")
    @Schema(description = "ID of the department the employee belongs to", example = "1")
    private Long departmentId;
}

package com.nik.kafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Schema(description = "Department response payload")
public class DepartmentResponse {

    @Schema(description = "Department ID", example = "1")
    private Long id;

    @Schema(description = "Department name", example = "Engineering")
    private String name;

    @Schema(description = "Department location", example = "New York")
    private String location;

    @Schema(description = "Number of employees in the department", example = "12")
    private int employeeCount;
}

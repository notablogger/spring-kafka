package com.training.kafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Schema(description = "Payload for creating or updating a department")
public class DepartmentRequest {

    @NotBlank(message = "Name is required")
    @Schema(description = "Department name", example = "Engineering")
    private String name;

    @NotBlank(message = "Location is required")
    @Schema(description = "Department location", example = "New York")
    private String location;
}

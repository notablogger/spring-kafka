package com.nik.kafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Schema(description = "Payload for creating or updating a department")
public class DepartmentRequest {

    @Schema(description = "Department name", example = "Engineering")
    private String name;

    @Schema(description = "Department location", example = "New York")
    private String location;
}

package com.training.kafka.mapstruct;

import com.training.kafka.avro.EmployeeEvent;
import com.training.kafka.entity.EmployeeEventDocument;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

import java.time.Instant;

@Mapper(
        componentModel = "spring",
        imports = {Instant.class}
)
public interface EmployeeEventToDocumentMapper {

    @Mapping(target = "id",                 ignore = true)
    @Mapping(target = "employeeId",         source = "id")
    @Mapping(target = "firstName",          source = "firstName")
    @Mapping(target = "lastName",           source = "lastName")
    @Mapping(target = "email",              source = "email")
    @Mapping(target = "salary",             source = "salary")
    @Mapping(target = "hireDate",           source = "hireDate")
    @Mapping(target = "departmentName",     source = "department.name")
    @Mapping(target = "departmentLocation", source = "department.location")
    @Mapping(target = "eventType",          expression = "java(event.getEventType().name())")
    @Mapping(target = "eventTimestamp",     expression = "java(Instant.ofEpochMilli(event.getEventTimestamp()))")
    @Mapping(target = "receivedAt",         expression = "java(Instant.now())")
    EmployeeEventDocument toDocument(EmployeeEvent event);
}


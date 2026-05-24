package com.training.kafka.mapstruct;

import com.training.kafka.avro.DepartmentInfo;
import com.training.kafka.avro.EmployeeEvent;
import com.training.kafka.avro.EventType;
import com.training.kafka.entity.Employee;
import org.mapstruct.*;

import java.time.Instant;

@Mapper(
        componentModel = "spring",
        imports = {EventType.class, Instant.class}
)
public interface EmployeeToEventMapper {

    @Mapping(target = "id",             source = "id")
    @Mapping(target = "firstName",      source = "firstName")
    @Mapping(target = "lastName",       source = "lastName")
    @Mapping(target = "email",          source = "email")
    @Mapping(target = "salary",         source = "salary")
    @Mapping(target = "hireDate",       source = "hireDate")
    @Mapping(target = "department",     expression = "java(mapDepartment(employee))")
    @Mapping(target = "eventType",      expression = "java(EventType.valueOf(eventType))")
    @Mapping(target = "eventTimestamp", expression = "java(Instant.now().toEpochMilli())")
    @Mapping(target = "departmentBuilder", ignore = true)
    EmployeeEvent toEvent(Employee employee, @Context String eventType);

    default DepartmentInfo mapDepartment(Employee employee) {
        return DepartmentInfo.newBuilder()
                .setId(employee.getDepartment().getId())
                .setName(employee.getDepartment().getName())
                .setLocation(employee.getDepartment().getLocation())
                .build();
    }
}

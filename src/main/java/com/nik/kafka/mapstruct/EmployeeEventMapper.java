package com.nik.kafka.mapstruct;

import com.nik.kafka.avro.EmployeeEvent;
import com.nik.kafka.entity.Department;
import com.nik.kafka.entity.Employee;
import org.mapstruct.*;

import java.time.Instant;
import java.time.LocalDate;


@Mapper(
        componentModel = "spring",
        imports = {LocalDate.class, Instant.class}
)
public interface EmployeeEventMapper {

    @Mapping(target = "id",         source = "id")
    @Mapping(target = "firstName",  source = "firstName")
    @Mapping(target = "lastName",   source = "lastName")
    @Mapping(target = "email",      source = "email")
    @Mapping(target = "salary",     source = "salary")
    @Mapping(target = "hireDate",   source = "hireDate")
    @Mapping(target = "department", expression = "java(mapDepartment(event))")
    @Mapping(target = "lastUpdated",expression = "java(Instant.now())")
    void updateEmployeeFromEvent(EmployeeEvent event, @MappingTarget Employee employee);

    default Department mapDepartment(EmployeeEvent event) {
        Department dept = new Department();
        dept.setId(event.getDepartment().getId());
        dept.setName(event.getDepartment().getName());
        dept.setLocation(event.getDepartment().getLocation());
        return dept;
    }
}

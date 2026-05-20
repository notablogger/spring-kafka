package com.nik.kafka.kafka;

import com.nik.kafka.avro.EmployeeEvent;
import com.nik.kafka.entity.Department;
import com.nik.kafka.entity.Employee;
import org.mapstruct.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;

@Mapper(componentModel = "spring")
public interface EmployeeEventMapper {

    @Mapping(target = "id",         source = "id")
    @Mapping(target = "firstName",  source = "firstName")
    @Mapping(target = "lastName",   source = "lastName")
    @Mapping(target = "email",      source = "email")
    @Mapping(target = "salary",     expression = "java(salaryFromBytes(event.getSalary()))")
    @Mapping(target = "hireDate",   expression = "java(LocalDate.ofEpochDay(event.getHireDate()))")
    @Mapping(target = "department", expression = "java(mapDepartment(event))")
    @Mapping(target = "lastUpdated",expression = "java(Instant.now())")
    void updateEmployeeFromEvent(EmployeeEvent event, @MappingTarget Employee employee);

    default BigDecimal salaryFromBytes(ByteBuffer buffer) {
        return new BigDecimal(new BigInteger(buffer.array()));
    }

    default Department mapDepartment(EmployeeEvent event) {
        Department dept = new Department();
        dept.setId(event.getDepartment().getId());
        dept.setName(event.getDepartment().getName());
        dept.setLocation(event.getDepartment().getLocation());
        return dept;
    }
}


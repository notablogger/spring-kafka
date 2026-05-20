package com.nik.kafka.mapstruct;

import com.nik.kafka.avro.DepartmentInfo;
import com.nik.kafka.avro.EmployeeEvent;
import com.nik.kafka.entity.Employee;
import org.mapstruct.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

@Mapper(componentModel = "spring")
public interface EmployeeToEventMapper {

    @Mapping(target = "id", source = "id")
    @Mapping(target = "firstName",      source = "firstName")
    @Mapping(target = "lastName",       source = "lastName")
    @Mapping(target = "email",          source = "email")
    @Mapping(target = "salary",         expression = "java(salaryToBytes(employee.getSalary()))")
    @Mapping(target = "hireDate",       expression = "java((int) employee.getHireDate().toEpochDay())")
    @Mapping(target = "department",     expression = "java(mapDepartment(employee))")
    @Mapping(target = "eventType",      expression = "java(EventType.valueOf(eventType))")
    @Mapping(target = "eventTimestamp", expression = "java(Instant.now().toEpochMilli())")
    EmployeeEvent toEvent(Employee employee, @Context String eventType);

    default ByteBuffer salaryToBytes(BigDecimal salary) {
        return ByteBuffer.wrap(salary.unscaledValue().toByteArray());
    }

    default DepartmentInfo mapDepartment(Employee employee) {
        return DepartmentInfo.newBuilder()
                .setId(employee.getDepartment().getId())
                .setName(employee.getDepartment().getName())
                .setLocation(employee.getDepartment().getLocation())
                .build();
    }
}

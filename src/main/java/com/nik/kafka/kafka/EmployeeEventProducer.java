package com.nik.kafka.kafka;

import com.nik.kafka.entity.Employee;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.io.File;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmployeeEventProducer {

    private final KafkaTemplate<String, GenericRecord> kafkaTemplate;

    @Value("${spring.kafka.topic.employee}")
    private String employeeTopic;

    @Value("${spring.avro.schema-path}")
    private String schemaPath;

    // Parsed once at startup, reused for every event
    private Schema schema;
    private Schema departmentSchema;
    private Schema eventTypeSchema;

    @PostConstruct
    void init() throws Exception {
        schema = new Schema.Parser().parse(new File(schemaPath));
        departmentSchema = schema.getField("department").schema();
        eventTypeSchema  = schema.getField("eventType").schema();
        log.info("Avro schema loaded from: {}", schemaPath);
    }

    public void sendEmployeeCreatedEvent(Employee employee) {
        sendEvent(employee, "CREATED");
    }

    public void sendEmployeeUpdatedEvent(Employee employee) {
        sendEvent(employee, "UPDATED");
    }

    public void sendEmployeeDeletedEvent(Employee employee) {
        sendEvent(employee, "DELETED");
    }

    private void sendEvent(Employee employee, String eventType) {
        try {
            GenericRecord departmentRecord = new GenericData.Record(departmentSchema);
            departmentRecord.put("id",       employee.getDepartment().getId());
            departmentRecord.put("name",     employee.getDepartment().getName());
            departmentRecord.put("location", employee.getDepartment().getLocation());

            GenericRecord employeeEvent = new GenericData.Record(schema);
            employeeEvent.put("id",             employee.getId());
            employeeEvent.put("firstName",      employee.getFirstName());
            employeeEvent.put("lastName",       employee.getLastName());
            employeeEvent.put("email",          employee.getEmail());
            employeeEvent.put("salary",         employee.getSalary().unscaledValue().toByteArray());
            employeeEvent.put("hireDate",       (int) employee.getHireDate().toEpochDay());
            employeeEvent.put("department",     departmentRecord);
            employeeEvent.put("eventType",      new GenericData.EnumSymbol(eventTypeSchema, eventType));
            employeeEvent.put("eventTimestamp", Instant.now().toEpochMilli());

            CompletableFuture<SendResult<String, GenericRecord>> future =
                    kafkaTemplate.send(employeeTopic, String.valueOf(employee.getId()), employeeEvent);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send EmployeeEvent [{}] for employee id={}: {}",
                            eventType, employee.getId(), ex.getMessage());
                } else {
                    log.info("EmployeeEvent [{}] sent for employee id={} → topic={}, partition={}, offset={}",
                            eventType, employee.getId(),
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });

        } catch (Exception e) {
            log.error("Error building EmployeeEvent [{}] for employee id={}: {}", eventType, employee.getId(), e.getMessage(), e);
        }
    }
}

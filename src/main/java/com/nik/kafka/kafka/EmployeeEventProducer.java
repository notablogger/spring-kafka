package com.nik.kafka.kafka;

import com.nik.kafka.entity.Employee;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.ByteBuffer;
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
        ClassPathResource resource = new ClassPathResource("message.avsc");
        schema = new Schema.Parser().parse(resource.getInputStream());
        departmentSchema = schema.getField("department").schema();
        eventTypeSchema  = schema.getField("eventType").schema();
        log.info("Avro schema loaded from classpath: message.avsc");
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
            employeeEvent.put("salary", ByteBuffer.wrap(employee.getSalary().unscaledValue().toByteArray()));
            employeeEvent.put("hireDate",       (int) employee.getHireDate().toEpochDay());
            employeeEvent.put("department",     departmentRecord);
            employeeEvent.put("eventType",      new GenericData.EnumSymbol(eventTypeSchema, eventType));
            employeeEvent.put("eventTimestamp", Instant.now().toEpochMilli());

            Message<GenericRecord> message = MessageBuilder
                    .withPayload(employeeEvent)
                    .setHeader(KafkaHeaders.TOPIC, employeeTopic)
                    .setHeader(KafkaHeaders.KEY, String.valueOf(employee.getId()))
                    .setHeader("eventType", eventType)
                    .build();

            CompletableFuture<SendResult<String, GenericRecord>> future =
                    kafkaTemplate.send(message);

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

package com.training.kafka.kafka;

import com.training.kafka.avro.EmployeeEvent;
import com.training.kafka.entity.Employee;
import com.training.kafka.mapstruct.EmployeeToEventMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmployeeEventProducer {

    private final KafkaTemplate<String, EmployeeEvent> kafkaTemplate;
    private final EmployeeToEventMapper employeeToEventMapper;

    @Value("${spring.kafka.topic.employee}")
    private String employeeTopic;

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
            EmployeeEvent event = employeeToEventMapper.toEvent(employee, eventType);

            Message<EmployeeEvent> message = MessageBuilder
                    .withPayload(event)
                    .setHeader(KafkaHeaders.TOPIC, employeeTopic)
                    .setHeader(KafkaHeaders.KEY, String.valueOf(employee.getId()))
                    .setHeader("eventType", eventType)
                    .build();

            CompletableFuture<SendResult<String, EmployeeEvent>> future = kafkaTemplate.send(message);

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
            log.error("Error building EmployeeEvent [{}] for employee id={}: {}",
                    eventType, employee.getId(), e.getMessage(), e);
        }
    }
}

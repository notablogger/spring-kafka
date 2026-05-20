package com.nik.kafka.kafka;

import com.nik.kafka.avro.EmployeeEvent;
import com.nik.kafka.mapstruct.EmployeeEventMapper;
import com.nik.kafka.repository.EmployeeRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmployeeEventConsumer {

    private final EmployeeRepository employeeRepository;
    private final EmployeeEventMapper employeeEventMapper;

    @KafkaListener(
            topics = "${spring.kafka.topic.employee}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(ConsumerRecord<String, EmployeeEvent> record) {
        String eventType = "UNKNOWN";
        try {
            // Extract eventType header
            Header header = record.headers().lastHeader("eventType");
            if (header != null) {
                eventType = new String(header.value(), StandardCharsets.UTF_8);
            }

            EmployeeEvent event = record.value();
            if (event == null) {
                log.warn("⚠️  Received null payload — topic={} partition={} offset={}",
                        record.topic(), record.partition(), record.offset());
                return;
            }

            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            log.info("📨 Employee Event Consumed");
            log.info("   Header  → eventType  : {}", eventType);
            log.info("   Topic   → {}  Partition: {}  Offset: {}",
                    record.topic(), record.partition(), record.offset());
            log.info("   Key     → {}", record.key());
            log.info("     id         : {}", event.getId());
            log.info("     firstName  : {}", event.getFirstName());
            log.info("     lastName   : {}", event.getLastName());
            log.info("     email      : {}", event.getEmail());
            log.info("     eventType  : {}", event.getEventType());
            log.info("     timestamp  : {}", event.getEventTimestamp());
            log.info("     department : {}", event.getDepartment());
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

            // ─── Map EmployeeEvent → Employee entity via MapStruct ──
            final String finalEventType = eventType;
            employeeRepository.findById(event.getId()).ifPresentOrElse(employee -> {
                employeeEventMapper.updateEmployeeFromEvent(event, employee);
                employeeRepository.save(employee);
                log.info("✅ Employee id={} fully updated from Kafka event [{}]", event.getId(), finalEventType);
            }, () -> log.warn("⚠️  Employee id={} not found in DB — skipping update", event.getId()));

        } catch (Exception e) {
            log.error("❌ Failed to consume EmployeeEvent [{}] — topic={} partition={} offset={} key={} : {}",
                    eventType, record.topic(), record.partition(), record.offset(), record.key(),
                    e.getMessage(), e);
        }
    }
}

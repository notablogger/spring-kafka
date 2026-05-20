package com.nik.kafka.kafka;

import com.nik.kafka.repository.EmployeeRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmployeeEventConsumer {

    private final EmployeeRepository employeeRepository;

    @KafkaListener(
            topics = "${spring.kafka.topic.employee}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(ConsumerRecord<String, GenericRecord> record) {
        String eventType = "UNKNOWN";
        try {
            // Extract eventType header
            Header header = record.headers().lastHeader("eventType");
            if (header != null) {
                eventType = new String(header.value(), StandardCharsets.UTF_8);
            }

            GenericRecord payload = record.value();
            if (payload == null) {
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
            log.info("     id         : {}", payload.get("id"));
            log.info("     firstName  : {}", payload.get("firstName"));
            log.info("     lastName   : {}", payload.get("lastName"));
            log.info("     email      : {}", payload.get("email"));
            log.info("     eventType  : {}", payload.get("eventType"));
            log.info("     timestamp  : {}", payload.get("eventTimestamp"));
            log.info("     department : {}", payload.get("department"));
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

            // ─── Update lastUpdated on the employee in DB ──────────
            Long employeeId = ((Number) payload.get("id")).longValue();
            employeeRepository.findById(employeeId).ifPresentOrElse(employee -> {
                employee.setLastUpdated(Instant.now());
                employeeRepository.save(employee);
                log.info("✅ lastUpdated set for employeeId={} eventType={}", employeeId, eventType);
            }, () -> log.warn("⚠️  Employee id={} not found in DB — skipping lastUpdated", employeeId));

        } catch (Exception e) {
            log.error("❌ Failed to consume EmployeeEvent [{}] — topic={} partition={} offset={} key={} : {}",
                    eventType, record.topic(), record.partition(), record.offset(), record.key(),
                    e.getMessage(), e);
        }
    }
}

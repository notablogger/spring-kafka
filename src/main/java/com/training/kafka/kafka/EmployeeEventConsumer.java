package com.training.kafka.kafka;

import com.training.kafka.avro.EmployeeEvent;
import com.training.kafka.entity.EmployeeEventDocument;
import com.training.kafka.mapstruct.EmployeeEventToDocumentMapper;
import com.training.kafka.repository.EmployeeEventDocumentRepository;
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

    private final EmployeeEventDocumentRepository eventDocumentRepository;
    private final EmployeeEventToDocumentMapper eventToDocumentMapper;

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
                log.warn("Received null payload — topic={} partition={} offset={}",
                        record.topic(), record.partition(), record.offset());
                return;
            }

            EmployeeEventDocument doc = eventToDocumentMapper.toDocument(event);
            eventDocumentRepository.save(doc);

            log.info("Saved EmployeeEvent [{}] for employee id={} to MongoDB", eventType, event.getId());

        } catch (Exception e) {
            log.error("Failed to consume EmployeeEvent [{}] — topic={} partition={} offset={}: {}",
                    eventType, record.topic(), record.partition(), record.offset(), e.getMessage(), e);
        }
    }
}

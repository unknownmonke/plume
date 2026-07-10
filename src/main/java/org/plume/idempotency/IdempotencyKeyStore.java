package org.plume.idempotency;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.plume.event.Event;

import java.util.List;
import java.util.Optional;

public interface IdempotencyKeyStore {

    Optional<IdempotencyKey> exists(ConsumerRecord<String, Event> consumerRecord, String groupId);

    Optional<IdempotencyKey> exists(ProducerRecord<String, Event> producerRecord);

    void save(ConsumerRecord<String, Event> consumerRecord, String groupId);

    void save(ProducerRecord<String, Event> producerRecord);

    List<IdempotencyKey> search(String key, String topic, String groupId);
}

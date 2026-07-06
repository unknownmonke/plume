package org.plume.producer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.plume.event.Event;
import org.plume.lifecycle.ShutdownManager;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;

@Slf4j
public class EventProducer {

    private final Map<?, ?> properties;
    private final ShutdownManager shutdownManager;

    private KafkaProducer<String, Event> kafkaProducer;
    private ProducerRecordBuilder producerRecordBuilder;


    public EventProducer(@NonNull Map<?, ?> properties, ShutdownManager shutdownManager) {
        log.info("Initializing producer...");

        this.properties = properties;
        this.shutdownManager = shutdownManager;

        setupProducer();
    }


    private void setupProducer() {
        Properties config = new Properties();
        config.putAll(properties);

        this.producerRecordBuilder = new ProducerRecordBuilder();
        this.kafkaProducer = new KafkaProducer<>(config);
        addShutdownHook();
    }

    /**
     * Adds provided hook or registers a default one that will stop the producer.
     */
    private void addShutdownHook() {
        if (shutdownManager == null) {
            // Registers default shutdown hook.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaProducer.close()));
        } else {
            // Registers provided hook
            shutdownManager.registerShutdownHook(() -> kafkaProducer.close());
        }
    }

    /* ------------------------------------------------------------ */
    /* Async publish with optional headers, partition and callback. */
    /* ------------------------------------------------------------ */

    public Future<RecordMetadata> publish(String topic, String key, Event event) {
        ProducerRecord<String, Event> producerRecord = producerRecordBuilder.buildRecord(topic, key, event);
        return publishRecord(producerRecord);
    }

    public Future<RecordMetadata> publish(String topic, String key, Event event,
                                          List<? extends Header> headers) {
        ProducerRecord<String, Event> producerRecord = producerRecordBuilder.buildRecord(topic, key, event, headers);

        return publishRecord(producerRecord);
    }

    public Future<RecordMetadata> publish(String topic, String key, Event event,
                                          Integer partition, List<? extends Header> headers) {
        ProducerRecord<String, Event> producerRecord = producerRecordBuilder.buildRecord(topic, key, event,partition, headers);

        return publishRecord(producerRecord);
    }

    public Future<RecordMetadata> publish(String topic, String key, Event event,Callback callback) {
        ProducerRecord<String, Event> producerRecord = producerRecordBuilder.buildRecord(topic, key, event);

        return publishRecord(producerRecord, callback);
    }

    public Future<RecordMetadata> publish(String topic, String key, Event event,
                                          List<? extends Header> headers, Callback callback) {
        ProducerRecord<String, Event> producerRecord = producerRecordBuilder.buildRecord(topic, key, event, headers);

        return publishRecord(producerRecord, callback);
    }

    public Future<RecordMetadata> publish(String topic, String key, Event event,
                                          Integer partition, List<? extends Header> headers, Callback callback) {
        ProducerRecord<String, Event> producerRecord = producerRecordBuilder.buildRecord(topic, key, event,partition, headers);

        return publishRecord(producerRecord, callback);
    }

    private Future<RecordMetadata> publishRecord(ProducerRecord<String, Event> producerRecord) {
        return publishRecord(producerRecord, (_, _) -> {});
    }

    private Future<RecordMetadata> publishRecord(ProducerRecord<String, Event> producerRecord, Callback callback) {
        Event event = producerRecord.value();

        return kafkaProducer.send(producerRecord, (metadata, exception) -> {
            // On success.
            if (Objects.isNull(exception)) {
                log.debug("Acknowledged record. \n Topic: {}\n Partition: {}\n Offset: {}\n Timestamp: {}",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    metadata.timestamp());
            }
            // On error.
            else {
                log.error("Error trying to publish : correlation id {}",
                    event.getMetadata().correlationId(), exception);
            }
            // Custom provided callback.
            callback.onCompletion(metadata, exception);
        });
    }
}
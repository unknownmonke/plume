package org.plume.consumer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.plume.event.Event;
import org.plume.event.EventFactory;
import org.plume.idempotency.IdempotencyKeyStore;
import org.plume.lifecycle.ShutdownManager;
import org.plume.lifecycle.StartupManager;
import org.plume.producer.InternalEventProducer;
import org.plume.producer.ProducerBootstrap;
import org.plume.security.Security;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.plume.common.Constants.getDlqTopic;

@Slf4j
public class EventConsumer implements Runnable {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final ConsumerBootstrap consumerBootstrap;
    private final BiConsumer<Headers, Event> consumerFunction;
    private final Security security;
    private final StartupManager startupManager;
    private final ShutdownManager shutdownManager;
    private final Map<?, ?> customProperties;
    private final boolean disableIdempotencyCheck;
    private final IdempotencyKeyStore idempotencyKeyStore;
    private final String dlqTopic;

    private KafkaConsumer<String, Event> kafkaConsumer;
    private InternalEventProducer internalProducer;


    // Basic constructor with only required values.
    public EventConsumer(@NonNull ConsumerBootstrap consumerBootstrap,
                         @NonNull BiConsumer<Headers, Event> consumerFunction,
                         @NonNull Security security,
                         Map<?, ?> customProperties) {
        this(consumerBootstrap, consumerFunction, security,
            null, null, customProperties, true, null, null);
    }

    public EventConsumer(@NonNull ConsumerBootstrap consumerBootstrap,
                         @NonNull BiConsumer<Headers, Event> consumerFunction,
                         @NonNull Security security,
                         StartupManager startupManager,
                         ShutdownManager shutdownManager,
                         Map<?, ?> customProperties,
                         boolean disableIdempotencyCheck,
                         IdempotencyKeyStore idempotencyKeyStore,
                         String dlqTopic
    ) {
        log.info("Initializing consumer...");

        this.consumerBootstrap = consumerBootstrap;
        this.consumerFunction = consumerFunction;
        this.security = security;
        this.startupManager = startupManager;
        this.shutdownManager = shutdownManager;
        this.customProperties = customProperties;
        this.disableIdempotencyCheck = disableIdempotencyCheck;
        this.idempotencyKeyStore = maybeSetIdempotencyStore(idempotencyKeyStore);
        this.dlqTopic = dlqTopic;

        setupConsumer();
    }


    private void setupConsumer() {
        this.kafkaConsumer = new KafkaConsumer<>(buildConfig());
        this.internalProducer = buildInternalProducer();
        addStartupHook();
        addShutdownHook();
    }

    private Properties buildConfig() {
        Properties config = new Properties();

        // Applies common configuration.
        config.putAll(consumerBootstrap.properties());

        // Applies security configuration.
        config.putAll(security.securityConfig());

        // Registers supplied custom properties.
        if (customProperties != null) {
            config.putAll(customProperties);
        }
        return config;
    }

    private InternalEventProducer buildInternalProducer() {
        ProducerBootstrap internalProducerBootstrap = new ProducerBootstrap(
            consumerBootstrap.getBootstrapServers(),
            consumerBootstrap.getClientId() + "-internal-producer"
        );

        return new InternalEventProducer(internalProducerBootstrap, security);
    }

    /**
     * Adds delaying startup hook if any.
     */
    private void addStartupHook() {
        if (startupManager != null) {
            startupManager.registerStartupHook(this);
        }
    }

    /**
     * Adds provided hook or registers a default one that will stop the consumer.
     */
    private void addShutdownHook() {
        if (shutdownManager == null) {
            // Registers default shutdown hook.
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        } else {
            // Registers provided hook.
            shutdownManager.registerShutdownHook(this);
        }
    }

    private IdempotencyKeyStore maybeSetIdempotencyStore(IdempotencyKeyStore idempotencyKeyStore) {
        if (idempotencyKeyStore == null) {
            if (!disableIdempotencyCheck) {
                throw new IllegalStateException(
                    "IdempotencyKeyStore implementation is required. " +
                        "Provide one in constructor " +
                        "or explicitly opt-out by setting disableIdempotencyCheck to true.");
            }
            log.warn("Consumer-side idempotency feature is explicitly disabled. "
                + "Duplicate events will not be detected and may be published more than once.");
        }
        return idempotencyKeyStore;
    }

    /**
     * Polls into a separate thread to not block main thread on infinite loop.
     */
    @Override
    public void run() {
        new Thread(() -> {
            try {
                kafkaConsumer.subscribe(consumerBootstrap.getTopics());
                log.info("Subscribed to topics: {}", kafkaConsumer.listTopics().keySet());

                while (!closed.get()) {
                    ConsumerRecords<String, Event> records = kafkaConsumer.poll(Duration.ofSeconds(5));

                    for (ConsumerRecord<String, Event> record : records) {
                        log.info("------- Processing record: key={}, topic={} at offset={} -------", record.key(), record.topic(), record.offset());
                        maybeProcessRecord(record);
                    }
                }
            } catch (WakeupException e) {
                // Ignores exception if closing.
                if (!closed.get()) throw e;

            } catch (Exception e) {
                log.error("Error in consumer thread:", e);

            } finally {
                kafkaConsumer.close();
                log.info("------- Consumer stopped -------");
            }
        }).start();
    }

    public void stop() {
        closed.set(true);
        kafkaConsumer.wakeup();
    }

    private void maybeProcessRecord(ConsumerRecord<String, Event> consumerRecord) {
        if (!disableIdempotencyCheck) {
            if (isDuplicate(consumerRecord)) {
                publishToDlq(consumerRecord);
            }
            idempotencyKeyStore.save(consumerRecord, consumerBootstrap.getGroupId());
        }
        consumerFunction.accept(consumerRecord.headers(), consumerRecord.value());
    }

    private boolean isDuplicate(ConsumerRecord<String, Event> consumerRecord) {

        if (idempotencyKeyStore.exists(consumerRecord, consumerBootstrap.getGroupId()).isPresent()) {

            log.info("⚠ Duplicate event detected: key={}, topic={} at offset={}",
                consumerRecord.key(), consumerRecord.topic(), consumerRecord.offset());
            return true;
        }
        return false;
    }

    private void publishToDlq(ConsumerRecord<String, Event> originalRecord) {
        String topic = getDlqTopic(dlqTopic, originalRecord.topic());
        String key = originalRecord.key();
        Event event = originalRecord.value();

        Event ignoredEvent = EventFactory.buildIgnoreEvent(event, "Duplicate event detected by consumer-side idempotency");

        log.info("(Publishing duplicate to DLQ: key={}, topic={})", key, topic);
        internalProducer.publish(topic, key, ignoredEvent);
    }
}

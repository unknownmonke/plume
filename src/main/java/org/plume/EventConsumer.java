package org.plume;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;
import static org.slf4j.LoggerFactory.getLogger;

public class EventConsumer {

    private static final Logger LOGGER = getLogger(EventConsumer.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private KafkaConsumer<String, String> kafkaConsumer;


    public EventConsumer(Map<?, ?> properties) {
        LOGGER.info("Setting up consumer...");

        requireNonNull(properties, "Properties cannot be null");

        setupConsumer(properties);
    }


    private void setupConsumer(Map<?, ?> properties) {
        Properties config = new Properties();
        config.putAll(properties);

        this.kafkaConsumer = new KafkaConsumer<>(config);
        addShutdownHook();
    }

    /**
     * Registers a simple shutdown hook that will stop the consumer.
     * Avoid joining the main / test thread here because joining threads from within the shutdown hook can deadlock the JVM shutdown
     * (the hook runs concurrently with other non-daemon threads).
     */
    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    public void run(List<String> topics, BiConsumer<Headers, String> consumerFunction) {

        requireNonNull(topics, "Topics cannot be null");
        requireNonNull(consumerFunction, "Consumer function cannot be null");

        // Polls into a separate thread to not block main thread on infinite loop.
        new Thread(() -> {
            try {
                kafkaConsumer.subscribe(topics);

                LOGGER.info("Subscribed to topics : {}", topics);

                while (!closed.get()) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        consumerFunction.accept(record.headers(), record.value());
                    }
                }
            } catch (WakeupException e) {
                // Ignores exception if closing.
                if (!closed.get()) throw e;

            } catch (Exception e) {
                LOGGER.error("Error in consumer thread.", e);

            } finally {
                kafkaConsumer.close();
                LOGGER.info("Consumer stopped.");
            }
        }).start();
    }

    public void stop() {
        closed.set(true);
        kafkaConsumer.wakeup();
    }
}

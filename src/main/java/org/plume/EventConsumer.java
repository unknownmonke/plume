package org.plume;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.plume.lifecycle.ShutdownManager;
import org.plume.lifecycle.StartupManager;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;

@Slf4j
public class EventConsumer implements Runnable {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final List<String> topics;
    private final BiConsumer<Headers, String> consumerFunction;
    private final StartupManager startupManager;
    private final ShutdownManager shutdownManager;

    private KafkaConsumer<String, String> kafkaConsumer;


    public EventConsumer(Map<?, ?> properties,
                         List<String> topics,
                         BiConsumer<Headers,String> consumerFunction,
                         StartupManager startupManager,
                         ShutdownManager shutdownManager
                         ) {
        log.info("Initializing consumer...");

        requireNonNull(properties, "Properties cannot be null");
        requireNonNull(topics, "Topics cannot be null");
        requireNonNull(consumerFunction, "Consumer function cannot be null");

        this.topics = topics;
        this.consumerFunction = consumerFunction;
        this.startupManager = startupManager;
        this.shutdownManager = shutdownManager;

        setupConsumer(properties);
    }


    private void setupConsumer(Map<?, ?> properties) {
        Properties config = new Properties();
        config.putAll(properties);

        this.kafkaConsumer = new KafkaConsumer<>(config);
        addStartupHook();
        addShutdownHook();
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

    /**
     * Polls into a separate thread to not block main thread on infinite loop.
     */
    @Override
    public void run() {
        new Thread(() -> {
            try {
                kafkaConsumer.subscribe(topics);

                log.info("Subscribed to topics : {}", topics);

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
                log.error("Error in consumer thread.", e);

            } finally {
                kafkaConsumer.close();
                log.info("Consumer stopped.");
            }
        }).start();
    }

    public void stop() {
        closed.set(true);
        kafkaConsumer.wakeup();
    }
}

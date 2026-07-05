package org.plume.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.plume.EventConsumer;
import org.plume.event.Event;
import org.plume.integration.common.AbstractIT;
import org.plume.integration.config.TestConsumerProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.plume.event.TestEventFactory.buildTestEvent;

public class EventConsumerTest extends AbstractIT {

    @Test
    void consumer_should_process_events() throws ExecutionException, InterruptedException {

        List<Event> values = new ArrayList<>();

        Properties properties = TestConsumerProperties.getProperties(kafkaContainer.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-eventconsumer-group");

        EventConsumer eventConsumer = new EventConsumer(
            properties,
            List.of(TOPIC),
            (_, value) -> values.add(value),
            null,
            null
            );

        producer.send(new ProducerRecord<>(TOPIC, "key", buildTestEvent())).get();

        eventConsumer.run();

        Awaitility
            .await()
            .atMost(Duration.ofSeconds(15))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                assertThat(values.isEmpty()).isFalse();
                eventConsumer.stop(); // Closes polling thread.
            });
    }
}

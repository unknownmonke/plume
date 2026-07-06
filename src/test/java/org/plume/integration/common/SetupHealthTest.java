package org.plume.integration.common;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.plume.event.Event;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.plume.event.TestEventFactory.buildTestEvent;

public class SetupHealthTest extends AbstractIT {

    @Test
    void should_init_cluster_correctly() throws ExecutionException, InterruptedException {

        // Verifies container is running.
        assert kafkaContainer.isRunning();

        // Verifies producer is up.
        producer.send(new ProducerRecord<>(TOPIC, "key", buildTestEvent())).get();

        // Verifies consumer is up.
        consumer.subscribe(Collections.singletonList(TOPIC));

        ConsumerRecords<String, Event> records = consumer.poll(Duration.ofSeconds(5));
        assert records.count() > 0;

        // Verifies admin client is up.
        Set<String> topics = adminClient.listTopics().names().get();
        assert topics.contains(TOPIC);
    }
}

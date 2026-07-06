package org.plume.integration.producer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.plume.event.Event;
import org.plume.integration.common.AbstractIT;
import org.plume.integration.config.TestProducerProperties;
import org.plume.producer.EventProducer;
import org.plume.security.PlainTextSecurity;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.plume.event.TestEventFactory.buildTestEvent;

public class EventProducerTest extends AbstractIT {

    @Test
    void producer_should_publish_event() throws ExecutionException, InterruptedException {
        Properties properties = TestProducerProperties.getProperties(kafkaContainer.getBootstrapServers());

        EventProducer eventProducer = new EventProducer(properties, new PlainTextSecurity(), null);

        eventProducer.publish(TOPIC, "key", buildTestEvent()).get();

        consumer.subscribe(Collections.singletonList(TOPIC));

        ConsumerRecords<String, Event> records = consumer.poll(Duration.ofSeconds(5));
        assert records.count() > 0;
    }
}

package org.plume.producer;

import lombok.ToString;
import org.apache.kafka.common.serialization.StringSerializer;
import org.plume.common.AbstractBootstrap;
import org.plume.security.Security;
import org.plume.serialization.EventSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.plume.common.Constants.ACKS;

/**
 * Encapsulates required and common configuration for producers.
 *
 * <p> Just implements {@code AbstractConfig} without adding anything for now.
 */
@ToString(callSuper = true)
public class ProducerBootstrap extends AbstractBootstrap {

    public ProducerBootstrap(String bootstrapServers, String clientId, Security security) {
        super(bootstrapServers, clientId, security);
    }


    @Override
    public Properties properties() {
        Properties properties = super.properties();
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, EventSerializer.class);
        properties.put(ACKS_CONFIG, ACKS);
        return properties;
    }
}

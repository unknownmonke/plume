package org.plume.consumer;

import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.plume.common.AbstractBootstrap;
import org.plume.security.Security;
import org.plume.serialization.EventDeserializer;

import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

/**
 * Encapsulates required and common configuration for consumers :
 *
 * <li> Group ID.
 * <li> Topics.
 */
@ToString(callSuper = true)
@Getter
public class ConsumerBootstrap extends AbstractBootstrap {

    private final String groupId;
    private List<String> topics;


    private ConsumerBootstrap(String bootstrapServers, String clientId, Security security, String groupId) {
        super(bootstrapServers, clientId, security);
        this.groupId = groupId;
    }


    public static ConsumerBootstrap forTopic(String bootstrapServers, String clientId, Security security,
                                             String groupId, String topic) {
        ConsumerBootstrap config = new ConsumerBootstrap(bootstrapServers, clientId, security, groupId);
        config.topics = List.of(topic);
        return config;
    }

    public static ConsumerBootstrap forTopic(String bootstrapServers, String clientId, Security security,
                                             String groupId, List<String> topics) {
        ConsumerBootstrap config = new ConsumerBootstrap(bootstrapServers, clientId, security, groupId);
        config.topics = topics;
        return config;
    }

    @Override
    public Properties properties() {
        Properties properties = super.properties();
        properties.put(GROUP_ID_CONFIG, this.groupId);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, EventDeserializer.class);
        return properties;
    }
}

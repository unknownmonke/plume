package org.plume.consumer;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
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
    private final List<String> topics;


    private ConsumerBootstrap(String bootstrapServers, String clientId, Security security, String groupId, List<String> topics) {
        super(bootstrapServers, clientId, security);
        this.groupId = groupId;
        this.topics = topics;
    }


    public static ConsumerBootstrapBuilder with(String bootstrapServers, String clientId, Security security, String groupId) {
        return new ConsumerBootstrapBuilder(bootstrapServers, clientId, security, groupId);
    }

    @Override
    public Properties properties() {
        Properties properties = super.properties();
        properties.put(GROUP_ID_CONFIG, this.groupId);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, EventDeserializer.class);
        return properties;
    }

    /* ------------------------------------------------------------------------ */

    /**
     * Restricted builder for optional arguments only.
     */
    @RequiredArgsConstructor
    public static class ConsumerBootstrapBuilder {

        private final String bootstrapServers;
        private final String clientId;
        private final Security security;
        private final String groupId;

        private List<String> topics;


        public ConsumerBootstrapBuilder forTopic(String topic) {
            this.topics = List.of(topic);
            return this;
        }

        public ConsumerBootstrapBuilder forTopics(String... topics) {
            this.topics = List.of(topics);
            return this;
        }

        public ConsumerBootstrapBuilder forTopics(List<String> topics) {
            this.topics = topics;
            return this;
        }

        public ConsumerBootstrap build() {
            return new ConsumerBootstrap(bootstrapServers, clientId, security, groupId, topics);
        }
    }
}

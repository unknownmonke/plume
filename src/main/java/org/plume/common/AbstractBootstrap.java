package org.plume.common;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Properties;

import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;

/**
 * Base class for required producers & consumers config :
 *
 * <li> Bootstrap servers.
 * <li> Client ID.
 */
@ToString
@Getter
@RequiredArgsConstructor
public abstract class AbstractBootstrap {

    final String bootstrapServers;
    final String clientId;

    public Properties properties() {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        properties.put(CLIENT_ID_CONFIG, this.clientId + "-" + randomUUID());

        return properties;
    }
}

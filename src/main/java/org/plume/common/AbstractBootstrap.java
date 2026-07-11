package org.plume.common;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.plume.security.Security;

import java.util.Properties;

import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;

/**
 * Base class for configuration required by both producers & consumers :
 *
 * <li> Bootstrap servers.
 * <li> Client ID.
 * <li> Security.
 */
@ToString
@Getter
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class AbstractBootstrap {

    final String bootstrapServers;
    final String clientId;
    final Security security;


    public Properties properties() {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        properties.put(CLIENT_ID_CONFIG, this.clientId + "-" + randomUUID());
        properties.putAll(security.securityConfig());
        return properties;
    }
}

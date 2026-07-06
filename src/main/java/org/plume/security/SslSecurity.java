package org.plume.security;

import lombok.ToString;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.*;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SSL;

@ToString(callSuper = true)
public class SslSecurity extends AbstractSecurity implements Security {

    private final String trustStoreLocation;
    private final String trustStorePassword;
    private final String keyStoreLocation;
    private final String keyStorePassword;


    public SslSecurity(SecurityStoreInfo trustStoreInfo, SecurityStoreInfo keyStoreInfo) {
        super(SSL);
        this.trustStoreLocation = trustStoreInfo.location();
        this.trustStorePassword = trustStoreInfo.password();
        this.keyStoreLocation = keyStoreInfo.location();
        this.keyStorePassword = keyStoreInfo.password();
    }


    @Override
    public Properties securityConfig() {
        Properties config = new Properties();
        config.put(SECURITY_PROTOCOL_CONFIG, protocol.name());
        config.put(SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreLocation);
        config.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
        config.put(SSL_KEYSTORE_LOCATION_CONFIG, keyStoreLocation);
        config.put(SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
        return config;
    }

    public record SecurityStoreInfo(String location, String password) {}
}

package org.plume.security;

import lombok.ToString;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT;

@ToString(callSuper = true)
public class PlainTextSecurity extends AbstractSecurity implements Security {

    public PlainTextSecurity() {
        super(PLAINTEXT);
    }

    @Override
    public Properties securityConfig() {
        Properties config = new Properties();
        config.put(SECURITY_PROTOCOL_CONFIG, protocol.name());
        return config;
    }
}

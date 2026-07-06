package org.plume.security;

import lombok.ToString;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;

@ToString(callSuper = true)
public class AbstractSaslSecurity extends AbstractSecurity implements Security {

    private static final String JAAS_TEMPLATE =
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";

    private final String jaasConfig;

    protected AbstractSaslSecurity(SecurityProtocol protocol, String username, String password) {
        super(protocol);
        this.jaasConfig = String.format(JAAS_TEMPLATE, username, password);
    }

    protected AbstractSaslSecurity(SecurityProtocol protocol, String jaasConfig) {
        super(protocol);
        this.jaasConfig = jaasConfig;
    }

    @Override
    public Properties securityConfig() {
        Properties config = new Properties();
        config.put(SECURITY_PROTOCOL_CONFIG, protocol.name());
        config.put(SASL_MECHANISM, "PLAIN");
        config.put(SASL_JAAS_CONFIG, jaasConfig);
        return config;
    }
}

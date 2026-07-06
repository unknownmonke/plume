package org.plume.security;

import static org.apache.kafka.common.security.auth.SecurityProtocol.SASL_SSL;

public class SaslSslSecurity extends AbstractSaslSecurity implements Security {

    public SaslSslSecurity(String username, String password) {
        super(SASL_SSL, username, password);
    }

    public SaslSslSecurity(String jaasConfig) {
        super(SASL_SSL, jaasConfig);
    }
}

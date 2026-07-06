package org.plume.security;

import static org.apache.kafka.common.security.auth.SecurityProtocol.SASL_PLAINTEXT;

public class SaslPlainTextSecurity extends AbstractSaslSecurity implements Security {

    public SaslPlainTextSecurity(String username, String password) {
        super(SASL_PLAINTEXT, username, password);
    }

    public SaslPlainTextSecurity(String jaasConfig) {
        super(SASL_PLAINTEXT, jaasConfig);
    }
}

package org.plume.security;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.kafka.common.security.auth.SecurityProtocol;

/**
 * Common class for protocol in all implementations.
 */
@ToString
@RequiredArgsConstructor
public abstract class AbstractSecurity {

    final SecurityProtocol protocol;
}

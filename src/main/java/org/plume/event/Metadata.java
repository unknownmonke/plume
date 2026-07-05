package org.plume.event;

import lombok.Builder;
import lombok.NonNull;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Event metadata. Each event must provide a UUID and correlationId for exactly-once semantics.
 */
@Builder
public record Metadata(
    @NonNull String uuid,
    @NonNull String correlationId,
    String parentId,
    LocalDateTime timestamp,
    Type type,
    Source source,
    Identity identity,
    String exposure,
    Map<?, ?> additionalProperties
) implements Serializable {}

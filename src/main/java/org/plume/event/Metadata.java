package org.plume.event;

import lombok.Builder;
import lombok.NonNull;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

/**
 * Event metadata. Each event must provide a UUID and correlationId for exactly-once semantics.
 */
@Builder
public record Metadata(
    @NonNull String uuid,
    @NonNull String correlationId,
    String parentId,
    Instant timestamp,
    Type type,
    Source source,
    Identity identity,
    Exposure exposure,
    Map<?, ?> additionalProperties
) implements Serializable {}

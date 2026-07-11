package org.plume.event;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

/**
 * Event metadata. Each event must provide a UUID and correlationId for exactly-once semantics.
 */
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
) implements Serializable {

    public static MetadataBuilder with(String uuid, String correlationId) {
        return new MetadataBuilder(uuid, correlationId);
    }

    @RequiredArgsConstructor
    public static class MetadataBuilder {

        private final String uuid;
        private final String correlationId;
        private String parentId;
        private Instant timestamp;
        private Type type;
        private Source source;
        private Identity identity;
        private Exposure exposure;
        private Map<?, ?> additionalProperties;


        public MetadataBuilder parentId(String parentId) {
            this.parentId = parentId;
            return this;
        }

        public MetadataBuilder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public MetadataBuilder type(Type type) {
            this.type = type;
            return this;
        }

        public MetadataBuilder source(Source source) {
            this.source = source;
            return this;
        }

        public MetadataBuilder identity(Identity identity) {
            this.identity = identity;
            return this;
        }

        public MetadataBuilder exposure(Exposure exposure) {
            this.exposure = exposure;
            return this;
        }

        public MetadataBuilder additionalProperties(Map<?, ?> additionalProperties) {
            this.additionalProperties = additionalProperties;
            return this;
        }

        public Metadata build() {
            return new Metadata(uuid, correlationId, parentId, timestamp,
                type, source, identity, exposure, additionalProperties
            );
        }
    }
}

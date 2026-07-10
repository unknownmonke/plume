package org.plume.idempotency;

import java.time.Instant;

public record IdempotencyKey(String key, String topic, String hash, Instant createdAt, String groupId) {

    public IdempotencyKey(String key, String topic, String hash, Instant createdAt) {
        this(key, topic, hash, createdAt, null);
    }
}

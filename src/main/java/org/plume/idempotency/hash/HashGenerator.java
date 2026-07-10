package org.plume.idempotency.hash;

import org.plume.event.Event;

@FunctionalInterface
public interface HashGenerator {

    /**
     * Generates a hash for a given event.
     * Used for idempotency control.
     */
    String hash(Event event);
}

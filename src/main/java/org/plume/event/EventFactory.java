package org.plume.event;

import java.time.Instant;

import static org.plume.event.Type.IGNORED;

public class EventFactory {

    public static Event buildIgnoreEvent(Event origin, String message) {
        return new Event(
            message,
            IGNORED,
            origin.getMetadata().source(),
            origin.getMetadata().identity(),
            Exposure.IGNORED,
            Instant.now(),
            origin.getMetadata().additionalProperties(),
            origin
        );
    }
}

package org.plume.event;

import java.time.Instant;

public class TestEventFactory {

    public static Event buildTestEvent() {
        return new Event(
            "payload",
            Type.INITIAL,
            new Source("app", "domain"),
            new Identity("identity"),
            Exposure.INITIAL,
            "1234",
            Instant.now(),
            null);
    }
}

package org.plume.event;

public class TestEventFactory {

    public static Event buildTestEvent() {
        return new Event(
            "payload",
            Type.INITIAL,
            Source.builder().app("app").domain("domain").build(),
            new Identity("identity", null),
            "1234",
            "ini",
            null);
    }
}

package org.plume.event;

public enum Exposure {
    INITIAL("ini"),
    IGNORED("dlq"),
    ERROR("err"),
    REPLAY("rep");

    private final String value;

    Exposure(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}

package org.plume.event;

import lombok.NonNull;

import java.io.Serializable;

/**
 * Holds information about event emitter within the organization.
 *
 * <p> Each event must have a source.
 *
 * @param app Trigram of the sending service.
 * @param domain Functional domain of the sending service.
 * @param component Specific service component (optional).
 */
public record Source(
    @NonNull String app,
    @NonNull String domain,
    String component
) implements Serializable {

    public Source(String app, String domain) {
        this(app, domain, null);
    }
}

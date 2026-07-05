package org.plume.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.plume.event.Event;
import tools.jackson.core.JacksonException;

import static org.plume.serialization.Mapper.OBJECT_MAPPER;

public class EventSerializer implements Serializer<Event> {

    @Override
    public byte[] serialize(String topic, Event event) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(event);

        } catch (JacksonException e) {
            throw new SerializationException("Error serializing event", e);
        }
    }
}

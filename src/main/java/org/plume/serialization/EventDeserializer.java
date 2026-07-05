package org.plume.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.plume.event.Event;
import tools.jackson.core.JacksonException;

import static org.plume.serialization.Mapper.OBJECT_MAPPER;

public class EventDeserializer implements Deserializer<Event> {

    @Override
    public Event deserialize(String topic, byte[] data) {
        try {
            return OBJECT_MAPPER.readValue(data, Event.class);

        } catch (JacksonException e) {
            throw new SerializationException("Error deserializing event", e);
        }
    }
}

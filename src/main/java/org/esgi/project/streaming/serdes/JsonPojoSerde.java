package org.esgi.project.streaming.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class JsonPojoSerde<T> extends Serdes.WrapperSerde<T> {

    public JsonPojoSerde(Class<T> targetType) {
        super(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(targetType));
    }

    public static <T> Serde<T> of(Class<T> targetType) {
        return new JsonPojoSerde<>(targetType);
    }
}

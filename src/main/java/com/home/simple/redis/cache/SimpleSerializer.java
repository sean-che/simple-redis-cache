package com.home.simple.redis.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.lang.NonNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@RequiredArgsConstructor
public class SimpleSerializer {

    private final ObjectMapper objectMapper;

    public byte[] serializeKey(@NonNull String keyPrefix, Object key) {
        if (key instanceof String) {
            return keyPrefix.concat((String) key).getBytes(StandardCharsets.UTF_8);
        } else {
            try {
                return keyPrefix.concat(objectMapper.writeValueAsString(key)).getBytes(StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {
                throw new SerializationException("Cannot serialize key: " + key.toString(), e);
            }
        }
    }

    public <T> T deserializeValue(byte[] value, Class<T> cacheType) {
        try {
            return objectMapper.readValue(value, cacheType);
        } catch (IOException e) {
            throw new SerializationException("Cannot deserialize value: ", e);
        }
    }

    public byte[] serializeValue(Object value) {
        try {
            return objectMapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Cannot serialize value: " + value.toString(), e);
        }
    }
}

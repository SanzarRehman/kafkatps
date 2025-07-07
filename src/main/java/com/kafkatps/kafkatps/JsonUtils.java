package com.kafkatps.kafkatps;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JsonUtils {
    // Use JsonMapper for better performance and configure once
    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .build();
    
    // Cache readers per class to avoid creating new ones - major performance boost
    private static final Map<Class<?>, ObjectReader> READER_CACHE = new ConcurrentHashMap<>();

    public static <T> T getMessage(byte[] message, Class<T> messageType) throws IOException {
        ObjectReader reader = READER_CACHE.computeIfAbsent(messageType, 
            clazz -> OBJECT_MAPPER.readerFor(clazz));
        return reader.readValue(message);
    }
}
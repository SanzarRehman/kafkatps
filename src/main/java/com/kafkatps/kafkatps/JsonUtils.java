package com.kafkatps.kafkatps;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import java.io.IOException;

public class JsonUtils {
  private static final ObjectReader
      objectReader = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).reader();

  public static <T> T getMessage(byte[] message, Class<T> messageType) throws IOException {
    return objectReader.readValue(message, messageType);
  }
}
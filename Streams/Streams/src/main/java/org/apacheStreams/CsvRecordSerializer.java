package org.apacheStreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class CsvRecordSerializer implements Serializer<CsvRecord> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, CsvRecord record) {
        try {
            return objectMapper.writeValueAsBytes(record);
        } catch (Exception e) {
            throw new SerializationException("Error serializing CSV record", e);
        }
    }
}

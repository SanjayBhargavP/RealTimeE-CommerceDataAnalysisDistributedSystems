package org.apacheStreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.StringReader;

public class CsvRecordDeserializer implements Deserializer<CsvRecord> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public CsvRecord deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try (CSVReader csvReader = new CSVReader(new StringReader(new String(data)))) {
            String[] values = csvReader.readNext();
            CsvRecord record = new CsvRecord();
            record.setEventTime(values[0]);
            record.setEventType(values[1]);
            record.setProductId(values[2]);
            record.setCategoryId(values[3]);
            record.setCategoryCode(values[4]);
            record.setBrand(values[5]);
            record.setPrice(Double.parseDouble(values[6]));
            record.setUserId(values[7]);
            record.setUserSession(values[8]);
            return record;
        } catch (Exception e) {
            throw new SerializationException("Error deserializing CSV record", e);
        }
    }
}
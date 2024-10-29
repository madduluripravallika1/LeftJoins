package com.example.serdes;

import com.example.model.Department;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class DepartmentSerde extends Serdes.WrapperSerde<Department> {
    public DepartmentSerde() {
        super(new DepartmentSerializer(), new DepartmentDeserializer());
    }

    public static class DepartmentSerializer implements Serializer<Department> {
        private ObjectMapper mapper = new ObjectMapper();

        @Override
        public byte[] serialize(String topic, Department data) {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing Department", e);
            }
        }
    }

    public static class DepartmentDeserializer implements Deserializer<Department> {
        private ObjectMapper mapper = new ObjectMapper();

        @Override
        public Department deserialize(String topic, byte[] data) {
            try {
                return mapper.readValue(data, Department.class);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing Department", e);
            }
        }
    }
}

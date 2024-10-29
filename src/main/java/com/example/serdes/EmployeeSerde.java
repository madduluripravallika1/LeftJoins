package com.example.serdes;

import com.example.model.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class EmployeeSerde extends Serdes.WrapperSerde<Employee> {
    public EmployeeSerde() {
        super(new EmployeeSerializer(), new EmployeeDeserializer());
    }

    public static class EmployeeSerializer implements Serializer<Employee> {
        private ObjectMapper mapper = new ObjectMapper();

        @Override
        public byte[] serialize(String topic, Employee data) {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing Employee", e);
            }
        }
    }

    public static class EmployeeDeserializer implements Deserializer<Employee> {
        private ObjectMapper mapper = new ObjectMapper();

        @Override
        public Employee deserialize(String topic, byte[] data) {
            try {
                return mapper.readValue(data, Employee.class);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing Employee", e);
            }
        }
    }
}

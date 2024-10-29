package com.example.serdes;


import com.example.model.EmployeeDepartment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class EmployeeDepartmentSerde extends Serdes.WrapperSerde<EmployeeDepartment> {
    public EmployeeDepartmentSerde() {
        super(new EmpDeptSerializer(), new EmpDeptDeserializer());
    }

    public static class EmpDeptSerializer implements Serializer<EmployeeDepartment> {
        private ObjectMapper mapper = new ObjectMapper();

        @Override
        public byte[] serialize(String topic, EmployeeDepartment data) {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing EmpDept", e);
            }
        }
    }

    public static class EmpDeptDeserializer implements Deserializer<EmployeeDepartment> {
        private ObjectMapper mapper = new ObjectMapper();

        @Override
        public EmployeeDepartment deserialize(String topic, byte[] data) {
            try {
                return mapper.readValue(data, EmployeeDepartment.class);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing EmpDept", e);
            }
        }
    }
}

package com.example.config;

import com.example.model.Department;
import com.example.model.Employee;
import com.example.model.EmployeeDepartment;
import com.example.serdes.DepartmentSerde;
import com.example.serdes.EmployeeDepartmentSerde;
import com.example.serdes.EmployeeSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean(name = "customKafkaStreamsConfig")
    public StreamsConfig kafkaStreamsConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "employee-department-join-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return new StreamsConfig(config);
    }

    @Bean
    public KStream<Integer, EmployeeDepartment> kStream(StreamsBuilder builder) {
        // Custom Serdes for Employee, Department, and EmployeeDepartment
        EmployeeSerde employeeSerde = new EmployeeSerde();
        DepartmentSerde departmentSerde = new DepartmentSerde();
        EmployeeDepartmentSerde employeeDepartmentSerde = new EmployeeDepartmentSerde();

        // Define the input streams
        KStream<Integer, Employee> employeeStream = builder.stream("employee-topic", Consumed.with(Serdes.Integer(), employeeSerde));
        KTable<Integer, Department> departmentTable = builder.table("department-topic", Consumed.with(Serdes.Integer(), departmentSerde));

        // Join the streams
        KStream<Integer, EmployeeDepartment> joinedStream = employeeStream.leftJoin(
                departmentTable,
                (employee, department) -> {
                    EmployeeDepartment result = new EmployeeDepartment();
                    result.setEmployeeId(employee.getId());
                    result.setEmployeeName(employee.getName());
                    result.setSalary(employee.getSalary());

                    // If department is null, default to "No Department" and set departmentId to 0
                    if (department != null) {
                        result.setDepartmentId(department.getId());
                        result.setDepartmentName(department.getName());
                    } else {
                        result.setDepartmentId(0); // or you can keep it unset
                        result.setDepartmentName("No Department");
                    }

                    return result;
                },
                Joined.with(Serdes.Integer(), employeeSerde, departmentSerde)
        );

        // Output to a topic
        joinedStream.to("employee-department-output-topic", Produced.with(Serdes.Integer(), employeeDepartmentSerde));

        return joinedStream;
    }
}


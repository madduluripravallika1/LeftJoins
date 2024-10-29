package com.example.producer;

import com.example.model.Department;
import com.example.model.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<Integer, Employee> employeeKafkaTemplate;
    private final KafkaTemplate<Integer, Department> departmentKafkaTemplate;

    @Autowired
    public KafkaProducerService(KafkaTemplate<Integer, Employee> employeeKafkaTemplate,
                                KafkaTemplate<Integer, Department> departmentKafkaTemplate) {
        this.employeeKafkaTemplate = employeeKafkaTemplate;
        this.departmentKafkaTemplate = departmentKafkaTemplate;
    }

    public void sendEmployee(Employee employee) {
        employeeKafkaTemplate.send("employee-topic", employee.getId(), employee);
    }

    public void sendDepartment(Department department) {
        departmentKafkaTemplate.send("department-topic", department.getId(), department);
    }
}

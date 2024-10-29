package com.example.controller;

import com.example.model.Department;
import com.example.model.Employee;
import com.example.producer.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private final KafkaProducerService kafkaProducerService;

    @Autowired
    public KafkaController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping("/employee")
    public ResponseEntity<String> sendEmployee(@RequestBody Employee employee) {
        kafkaProducerService.sendEmployee(employee);
        return ResponseEntity.ok("Employee sent to Kafka");
    }

    @PostMapping("/department")
    public ResponseEntity<String> sendDepartment(@RequestBody Department department) {
        kafkaProducerService.sendDepartment(department);
        return ResponseEntity.ok("Department sent to Kafka");
    }
}

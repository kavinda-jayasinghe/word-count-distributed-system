package lk.example.word_count_distributed_system.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.UUID;
@Slf4j
@Service
public class NodeRegistrationServiceImpl {
    @Value("${server.port}")
    private  String port;
    @Value("${server.node.value}")
    private  String value;
    private static final String NODE_REGISTRATION_TOPIC = "node_registration_topic";
    public static final String NODE_ID = UUID.randomUUID().toString();
    private final KafkaTemplate<String, String> kafkaTemplate;


    public NodeRegistrationServiceImpl(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    public void registerNode() {

        kafkaTemplate.send(NODE_REGISTRATION_TOPIC, NODE_ID +" "+ port +" "+ value );
        log.info("Node {} has registered in port {}.", NODE_ID,port);
    }
    public void registerAll() {
        kafkaTemplate.send(NODE_REGISTRATION_TOPIC,  UUID.randomUUID().toString() +" "+ 8080 +" "+ 1 );
        kafkaTemplate.send(NODE_REGISTRATION_TOPIC,  UUID.randomUUID().toString() +" "+ 8081 +" "+ 2 );
        kafkaTemplate.send(NODE_REGISTRATION_TOPIC,  UUID.randomUUID().toString() +" "+ 8082 +" "+ 3 );
        kafkaTemplate.send(NODE_REGISTRATION_TOPIC,  UUID.randomUUID().toString() +" "+ 8083 +" "+ 4 );
        kafkaTemplate.send(NODE_REGISTRATION_TOPIC,  UUID.randomUUID().toString() +" "+ 8084 +" "+5 );
        kafkaTemplate.send(NODE_REGISTRATION_TOPIC,  UUID.randomUUID().toString() +" "+ 8085 +" "+ 6 );

        log.info("Node {} has registered in port {}.", NODE_ID,port);
    }



}

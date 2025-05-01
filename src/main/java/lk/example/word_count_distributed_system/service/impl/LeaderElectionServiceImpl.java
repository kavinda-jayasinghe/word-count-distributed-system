////package lk.nuwansaranath.word_count_distributed_system.service.impl;
////
////import org.apache.kafka.clients.consumer.ConsumerRecord;
////import org.apache.kafka.clients.consumer.ConsumerRecords;
////import org.apache.kafka.clients.consumer.KafkaConsumer;
////import org.springframework.kafka.annotation.KafkaListener;
////import org.springframework.kafka.core.KafkaTemplate;
////import org.springframework.stereotype.Service;
////
////import java.time.Duration;
////import java.util.Collections;
////
////@Service
////public class LeaderElectionServiceImpl {
////
////    private static final String LEADER_ELECTION_TOPIC = "leader-election";
////    private static final String NODE_REGISTRATION_TOPIC = "node-registration";
////    private static final String LEADER_MESSAGE = "LEADER";
////    private static final int REQUIRED_NODE_COUNT = 4;
////
////    private final KafkaTemplate<String, String> kafkaTemplate;
////    private final KafkaConsumer<String, String> kafkaConsumer;
////
////    private final NodeRegistraionSeriveImpl nodeRegistrationService;
////
////    public LeaderElectionServiceImpl(KafkaTemplate<String, String> kafkaTemplate, KafkaConsumer<String, String> kafkaConsumer,
////                                     NodeRegistraionSeriveImpl nodeRegistrationService) {
////        this.kafkaTemplate = kafkaTemplate;
////        this.kafkaConsumer = kafkaConsumer;
////        this.nodeRegistrationService = nodeRegistrationService;
////    }
////
////    public void startLeaderElection() {
////        // Register the node in Kafka's node-registration topic
////        nodeRegistrationService.registerNode();
////
////        // Start the consumer thread to listen for leader messages and node registration
////        new Thread(this::consumeLeaderMessages).start();
////
////        // Ensure there are at least 4 nodes before starting leader election
////        if (checkNodeAvailability()) {
////            attemptToClaimLeadership();
////        } else {
////            System.out.println("Not enough nodes to start leader election.");
////        }
////    }
////
////    private boolean checkNodeAvailability() {
////        int nodeCount = 0;
////        kafkaConsumer.subscribe(Collections.singletonList(NODE_REGISTRATION_TOPIC));
////        while (nodeCount < REQUIRED_NODE_COUNT) {
////            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
////            nodeCount += records.count();
////            if (nodeCount >= REQUIRED_NODE_COUNT) {
////                System.out.println("Sufficient nodes available for leader election.");
////                return true;
////            }
////        }
////        return false;
////    }
////
////    private void consumeLeaderMessages() {
////        kafkaConsumer.subscribe(Collections.singletonList(LEADER_ELECTION_TOPIC));
////        while (true) {
////            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
////            for (ConsumerRecord<String, String> record : records) {
////                if (LEADER_MESSAGE.equals(record.value())) {
////                    // Detect the current leader
////                    System.out.println("Detected leader: " + record.value());
////                    return;
////                }
////            }
////        }
////    }
////
////    private void attemptToClaimLeadership() {
////        // Here you can add logic to ensure that only one node becomes the leader
////        if (isEligibleForLeadership()) {
////            // Claim leadership by sending a LEADER message to the topic
////            kafkaTemplate.send(LEADER_ELECTION_TOPIC, LEADER_MESSAGE);
////            System.out.println("This node is now the leader!");
////        } else {
////            System.out.println("This node is not the leader.");
////        }
////    }
////
////    private boolean isEligibleForLeadership() {
////        // Logic to check if this node is eligible for leadership (e.g., by checking if it's the first to start)
////        return true;  // Implement your eligibility criteria.
////    }
////    // KafkaListener to consume leader election messages
////    @KafkaListener(topics = LEADER_ELECTION_TOPIC, groupId = "leader-election")
////    public void listenForLeaderMessages(String message) {
////        if (LEADER_MESSAGE.equals(message)) {
////            // Detect the current leader
////            System.out.println("Detected leader: " + message);
////        }
////    }
////
////    // KafkaListener to consume node registration messages
////    @KafkaListener(topics = NODE_REGISTRATION_TOPIC, groupId = "node-registration")
////    public void listenForNodeRegistrations(String message) {
////        // Logic for handling node registration
////        System.out.println("Node registered: " + message);
////    }
////}
//
//package lk.nuwansaranath.word_count_distributed_system.service.impl;
//
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.stereotype.Service;
//
//import java.time.Duration;
//import java.util.Collections;
//import java.util.concurrent.atomic.AtomicInteger;
//
//@Service
//public class LeaderElectionServiceImpl {
//
//    private static final String LEADER_ELECTION_TOPIC = "leader-election";
//    private static final String NODE_REGISTRATION_TOPIC = "node-registration";
//    private static final String LEADER_MESSAGE = "LEADER";
//    private static final int REQUIRED_NODE_COUNT = 4;
//    private final AtomicInteger nodeCount = new AtomicInteger(0);
//
//    private final KafkaTemplate<String, String> kafkaTemplate;
//    private final NodeRegistraionSeriveImpl nodeRegistrationService;
//
//    public LeaderElectionServiceImpl(KafkaTemplate<String, String> kafkaTemplate,
//                                     NodeRegistraionSeriveImpl nodeRegistrationService) {
//        this.kafkaTemplate = kafkaTemplate;
//        this.nodeRegistrationService = nodeRegistrationService;
//    }
//
//    public void startLeaderElection() {
//        // Register the node in Kafka's node-registration topic
//        nodeRegistrationService.registerNode();
//
//        // Ensure there are at least 4 nodes before starting leader election
//        if (checkNodeAvailability()) {
//            attemptToClaimLeadership();
//        } else {
//            System.out.println("Not enough nodes to start leader election.");
//        }
//    }
//
//    private boolean checkNodeAvailability() {
//        int waited = 0;
//        int maxWait = 10000; // Wait up to 10 seconds
//        int interval = 500;  // Check every 500 milliseconds
//
//        while (nodeCount.get() < REQUIRED_NODE_COUNT && waited < maxWait) {
//            System.out.println("Waiting for nodes to register... Current count: " + nodeCount.get());
//            try {
//                Thread.sleep(interval);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                return false;
//            }
//            waited += interval;
//        }
//
//        if (nodeCount.get() >= REQUIRED_NODE_COUNT) {
//            System.out.println("Sufficient nodes available for leader election.");
//            return true;
//        } else {
//            System.out.println("Timeout waiting for nodes. Only " + nodeCount.get() + " node(s) registered.");
//            return false;
//        }
//    }
//
//    private void attemptToClaimLeadership() {
//        // Here you can add logic to ensure that only one node becomes the leader
//        if (isEligibleForLeadership()) {
//            // Claim leadership by sending a LEADER message to the topic
//            kafkaTemplate.send(LEADER_ELECTION_TOPIC, LEADER_MESSAGE);
//            System.out.println("This node is now the leader!");
//        } else {
//            System.out.println("This node is not the leader.");
//        }
//    }
//
//    private boolean isEligibleForLeadership() {
//        // Logic to check if this node is eligible for leadership
//        return true;  // Implement your eligibility criteria.
//    }
//
//    // KafkaListener to consume leader election messages
//    @KafkaListener(topics = LEADER_ELECTION_TOPIC, groupId = "leader-election")
//    public void listenForLeaderMessages(String message) {
//        if (LEADER_MESSAGE.equals(message)) {
//            // Detect the current leader
//            System.out.println("Detected leader: " + message);
//        }
//    }
//
//    // KafkaListener to consume node registration messages
//    @KafkaListener(topics = NODE_REGISTRATION_TOPIC, groupId = "node-registration")
//    public void listenForNodeRegistrations(String message) {
//        // Logic for handling node registration
//        System.out.println("Node registered: " + message);
//    }
//}
//

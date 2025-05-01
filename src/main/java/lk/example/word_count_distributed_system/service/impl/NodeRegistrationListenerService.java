package lk.example.word_count_distributed_system.service.impl;
import jakarta.transaction.Transactional;
import lk.example.word_count_distributed_system.entity.LeaderElection;
import lk.example.word_count_distributed_system.repository.LeaderElectionRepository;
import lk.example.word_count_distributed_system.utils.enums.Role;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Service
@RequiredArgsConstructor
public class NodeRegistrationListenerService {
    @Value("${server.node.value}")
    private  Integer value;
    @Value("${server.port}")
    private  Long port;
    private static final ArrayList<String> registeredNodes =new ArrayList<>();
    private  ArrayList<String> lineList =new ArrayList<>();
    private static final ArrayList<Integer> registeredNodesValues = new ArrayList<>();
    private static volatile String electedLeader = null;
    private final LeaderElectionRepository leaderElectionRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private String nodeId = NodeRegistrationServiceImpl.NODE_ID;
    private Role role;
    private HashMap<String,Role> nodeRole = new HashMap<>();
    private ConcurrentHashMap<String,Date> lastHearBeatTimeMap = new ConcurrentHashMap<>();
    private HashMap<String,Integer> wordCountForEachAcceptor = new HashMap<>();
    private HashMap<Long,ArrayList<String>> tempWordList = new HashMap<>();
    private HashMap<Character,ArrayList<String>> letterWordCount = new HashMap<>();
    private HashMap<Role,ArrayList<String>> nodeForRoleHashMap = new HashMap<>();
    private HashMap<Long,Long> wordCountForLine = new HashMap<>();
    private Date lastSendTimeStamp=null;
    private Integer totalAcceptors =0;
    private Integer count =1;
    private Integer lineNumber =1;
    private int startChar;
    private int endChar;
    private int starAsciNumber=60;

    @KafkaListener(topics = "node_registration_topic",  groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void handleNodeRegistration(ConsumerRecord<String, String> consumerRecord) {
        String message = consumerRecord.value();
        String nId = message.split(" ")[0];
        log.info("<<<<<<< Received node registration. port : {} ,Node id : {} >>>>>>>",message.split(" ")[1], nId);
        registeredNodes.add(nId);
        registeredNodesValues.add(Integer.valueOf(message.split(" ")[2]));
        log.info("<<<<<< Current registered nodes count: {} >>>>>>", registeredNodes.size());

        int randomMillis = ThreadLocalRandom.current().nextInt(0, 3000);
        try {
            Thread.sleep(randomMillis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Optional<LeaderElection> leaderElectionByIsLeaderCrashFalse = leaderElectionRepository.getExistLeader();
        if (leaderElectionByIsLeaderCrashFalse.isEmpty() && registeredNodes.size() >= 4 && electedLeader == null) {
            LeaderElection leaderElection = new LeaderElection();
            leaderElection.setElectedDate(new Date());
            leaderElection.setIsLeaderCrash(false);
            leaderElection.setNode(nodeId);
            leaderElection.setPort(port);
            leaderElectionRepository.save(leaderElection);
            log.info("<<<<<<< Election has been started by node  - {} port {} >>>>>>",nodeId,port);
            startElection();
        }  else if(Objects.nonNull(electedLeader)){
            if(Role.COORDINATOR.equals(role)){
//                assignRoleForNewNode(nId);
                roleAssign();
            }

        }else if(registeredNodes.size() < 4){
//            leaderElectionRepository.deleteElectionStartNode();
        }

    }

    public void startElection() {
        boolean higherNodeExists = false;
        for (int id : registeredNodesValues) {
            if (id > value && Objects.isNull(electedLeader) && registeredNodesValues.size()>=4) {
                int index = registeredNodesValues.indexOf(id);
                String nId = registeredNodes.get(index);
                log.info("startElection {} {} {} {}",nId,value,port,nodeId);
                kafkaTemplate.send("node_election_topic", nId+" "+value+" "+port+" "+nodeId);
                higherNodeExists = true;
            }
        }
        if (!higherNodeExists) {
            announceCoordinator(nodeId);
        }
    }

    private void assignLetterRangesToProcessors() {
        ArrayList<String> proposers = nodeForRoleHashMap.get(Role.PROPOSER);

        if (proposers == null || proposers.isEmpty()) {
            log.error("No proposer nodes to assign letters!");
            return;
        }

//        List<Character> letters = new ArrayList<>();
//        for (char c = 'A'; c <= 'Z'; c++) {
//            letters.add(c);
//        }

        int proposerCount = proposers.size();
        int lettersPerProposer = 26 / proposerCount;
        int extraLetters = 26 % proposerCount;
        log.info("node count {} lettersPerProposer {} extraLetters {}",proposerCount,lettersPerProposer,extraLetters);

        int index = 0;
        int start=starAsciNumber;
        int end=0;
        for (String proposerId : proposers) {
            end=start+lettersPerProposer;

            if (extraLetters > 0) {
                end=end+1;
                extraLetters--;
            }
            kafkaTemplate.send("assign_letter_range_topic", start +" "+end +" "+proposerId);
            log.info("Assigned letters from {} to {} processor {}", start,end, proposerId);
        }
    }
    public void announceCoordinator(String nodeId) {
        kafkaTemplate.send("coordinator_topic", port +" "+nodeId);
        role=Role.COORDINATOR;
        electedLeader=nodeId;
        log.info("<<<<I am the new leader: {} port = {}>>>>>", nodeId,port);
        roleAssign();
    }

    @KafkaListener(topics = "assign_letter_range_topic", groupId = "${spring.kafka.consumer.group-id}")
    public void setLetterRange(String message){
        log.info("setLetterRange {}",message);
        String[] strings = message.split(" ");
        if(strings[2].equals(nodeId) ) {
            startChar = Integer.parseInt(strings[0]);
            endChar = Integer.parseInt(strings[1]);

        }
    }
    @KafkaListener(topics = "node_election_topic", groupId = "${spring.kafka.consumer.group-id}")
    public void onElection(String message) {
        String[] strings = message.split(" ");
//        log.info("onElection {} {}",message,Objects.isNull(electedLeader));
//        log.info("strings[0].equals(nodeId) {} node id{}  sent nodeid {}",strings[0].equals(nodeId),nodeId,strings[0]);
        if(strings[0].equals(nodeId) && Objects.isNull(electedLeader)) {
            int fromPort = Integer.parseInt(strings[2]);
            log.info("<<<<<<< Election has been handed over from port - {} to {} >>>>>>", fromPort, port);
            int senderValue = Integer.parseInt(strings[1]);
            if (senderValue < value) {
                kafkaTemplate.send("node_election_forward_topic", String.valueOf(value), "OK from " + nodeId + " " + port+" "+strings[3]);
                if (Objects.isNull(electedLeader)) {
                    startElection();
                }
            }
        }
    }
    @KafkaListener(topics = "node_election_forward_topic", groupId = "${spring.kafka.consumer.group-id}")
    public void onElectionForward(String message) {
        String[] strings = message.split(" ");
        if(strings[4].equals(nodeId) && Objects.isNull(electedLeader)) {
            log.info("<<<<<<< Election has been continued by port {} >>>>>>", strings[3]);
        }
    }

    @KafkaListener(topics = "coordinator_topic", groupId = "${spring.kafka.consumer.group-id}")
    public void onCoordinator(String message) {
        String leaderId = message.split(" ")[1];
        if(!leaderId.equals(nodeId) && Objects.isNull(electedLeader)) {
            String leaderPort = message.split(" ")[0];
            log.info("<<<<< Leader selected, port = {} ,node id = {} >>>>>",leaderPort, leaderId);
            electedLeader=leaderId;
        }
    }
    @KafkaListener(topics = "processor_assign_topic", groupId = "${spring.kafka.consumer.group-id}")
    public void onProcessorAssign(String message) {
        String[] processorIds = message.split(" ");
        for(String processor :processorIds){
            if(nodeId.equals(processor)){
                role=Role.PROPOSER;
                log.info("<<<I am a processor. port {}>>>",port);
            }
        }
    }
    @KafkaListener(topics = "acceptor_assign_topic", groupId = "${spring.kafka.consumer.group-id}")
    public void onAcceptorAssign(String message) {
        String[] acceptorIds = message.split(" ");
        for(String acceptor :acceptorIds){
            if(nodeId.equals(acceptor)){
                role=Role.ACCEPTOR;
                log.info("<<<I am a acceptor. port {}>>>",port);
            }
        }
    }
    @KafkaListener(topics = "learner_assign_topic", groupId = "${spring.kafka.consumer.group-id}")
    public void onLearnerAssign(String message) {
        if(message.equals(nodeId)){
            role=Role.LEARNER;
            log.info("<<<I am a learner. port {}>>>",port);
            log.info("Counting words ...");
        }

    }
    private void assignRoleForNewNode(String nodeId){
        ArrayList<String> proposalNode = nodeForRoleHashMap.get(Role.PROPOSER);
        ArrayList<String> acceptorNode = nodeForRoleHashMap.get(Role.ACCEPTOR);
        if(acceptorNode.size()<=proposalNode.size()){
            acceptorNode.add(nodeId);
            nodeForRoleHashMap.put(Role.ACCEPTOR,acceptorNode);
            kafkaTemplate.send("acceptor_assign_topic", nodeId);

        }else{
            proposalNode.add(nodeId);
            nodeForRoleHashMap.put(Role.PROPOSER,proposalNode);
            kafkaTemplate.send("processor_assign_topic", nodeId);
        }
    }
    private void roleAssign(){
        if(Role.COORDINATOR.equals(role)){
//            lastHearBeatTimeMap.clear();
//            log.info("Start role assignment {}",port);
            List<String> allNodes = new ArrayList<>(registeredNodes.size());
            for(String node:registeredNodes){
//                log.info("Start role node {}",node);
                allNodes.add(node);
            }
            allNodes.remove(nodeId);
            Collections.shuffle(allNodes);
            String learnerNode =allNodes.get(0);
//            log.info("Start role 0 {}",learnerNode);
            allNodes.remove(learnerNode);
//            log.info("Start role 1 {}",learnerNode);
            nodeRole.put(learnerNode,Role.LEARNER);
//            log.info("Start role 2 {}",learnerNode);
            ArrayList<String> learnerNodes = new ArrayList<>();
            learnerNodes.add(learnerNode);
            nodeForRoleHashMap.put(Role.LEARNER,learnerNodes);
//            log.info("Start role learnerNode {}",learnerNode);
            kafkaTemplate.send("learner_assign_topic", learnerNode);
            int totalRemaining = allNodes.size();
            int proposerCount = (totalRemaining ) / 2;
            String acceptorMessage="";
            String processorMessage="";
            for (int i = 0; i < totalRemaining; i++) {
                String node = allNodes.get(i);
                if (i < proposerCount) {
                    nodeRole.put(node,Role.PROPOSER);
                    ArrayList<String> nodes = new ArrayList<>();
                    nodes.add(node);
                    nodeForRoleHashMap.put(Role.PROPOSER,nodes);
                    processorMessage=processorMessage +" "+node;
                    log.info("Start role processorMessage {} node size {}",node, nodes.size());
                } else {
                    ArrayList<String> nodes = new ArrayList<>();
                    nodes.add(node);
                    nodeRole.put(node,Role.ACCEPTOR);
                    nodeForRoleHashMap.put(Role.ACCEPTOR,nodes);
                    acceptorMessage=acceptorMessage+" "+node;
                    kafkaTemplate.send("acceptor_count", node);
                }
            }
            kafkaTemplate.send("acceptor_assign_topic", acceptorMessage);
            kafkaTemplate.send("processor_assign_topic", processorMessage);
            for(String node:registeredNodes){
                lastHearBeatTimeMap.put(node,new Date());
            }
            assignLetterRangesToProcessors();
            readFile();
        }

    }

    private void readFile(){
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("assignment.pdf");
            if (inputStream == null) {
                log.error("PDF file not found in resources!");
                return;
            }
            PDDocument document = PDDocument.load(inputStream);
            PDFTextStripper pdfStripper = new PDFTextStripper();
            String text = pdfStripper.getText(document);
            BufferedReader reader = new BufferedReader(new StringReader(text));
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    lineList.add(line);

                }
            }
            document.close();
            log.info("Completed file reading.");
            sendLine("1");
//            log.info("End reading");
//
        } catch (Exception e) {
            log.error("Error reading PDF document", e);
        }
    }

//    @KafkaListener(topics = "send_line_topic_coardinator",  groupId = "${spring.kafka.consumer.group-id}")
    public void sendLine(String message) {
//        log.info("send line message {} {}",message,lineNumber);
        lineNumber=Integer.parseInt(message);
        if(role.equals(Role.COORDINATOR) ){
            if(lineList.size()<lineNumber){
//                log.info(" if(lineList.size()<lineNumber) {} {}",message,lineNumber);

            }else{
                log.info("else; {}",lineList.get(lineNumber-1));
                kafkaTemplate.send("proposal_word_send_topic",lineList.size()+" "+lineNumber+" "+  lineList.get(lineNumber-1));
            }

        }

    }
    @KafkaListener(topics = "complete_line_topic",  groupId = "${spring.kafka.consumer.group-id}")
    public void completeReadingLine(String message) {
        if(Role.COORDINATOR.equals(role) ){
//            log.info("oldLine {}",lineNumber);
            int newLine = lineNumber+1;
//            log.info("newline {}",newLine);
            sendLine(String.valueOf(newLine));
        }

    }





    private int proposalNumberCounter = 0;
    private int highestProposalNumber = 0;
    private final Map<Integer, Integer> proposalMap = new HashMap<>();

    private int generateProposalNumber() {
        proposalNumberCounter++;
        return proposalNumberCounter;
    }
    @KafkaListener(topics = "acceptor_count",  groupId = "${spring.kafka.consumer.group-id}")
    public void acceptorCount(String message) {
        totalAcceptors=totalAcceptors+1;
    }

    @KafkaListener(topics = "proposal_word_send_topic",  groupId = "${spring.kafka.consumer.group-id}")
    public void proposal(String message) {
        if(Role.PROPOSER.equals(role)){
//            log.info("message - {} port - {}",message,port);
            String[] words = message.split(" ");
            String lineNumber = words[1];
            String totalLine = words[0];
            List<String> wordsWithoutFirstTwo = Arrays.asList(words).subList(2, words.length);
            for (String word : wordsWithoutFirstTwo) {
                int proposalNumber = generateProposalNumber();
//                log.info("word.toUpperCase().charAt(0) - {} start - {} end {}",word.toUpperCase().charAt(0),startChar,endChar);
                if(word.toUpperCase().charAt(0)<=endChar && word.toUpperCase().charAt(0)>=startChar){
//                    log.info("Word - {} port - {}",word,port);
                    kafkaTemplate.send("send_word_to_acceptor_node_topic",totalLine+" "+ lineNumber+" " +word +" "+proposalNumber+" "+nodeId);
                }
            }
            kafkaTemplate.send("complete_line_topic",  "Completed");
        }

    }
    @KafkaListener(topics = "proposal_promise_topic",  groupId = "${spring.kafka.consumer.group-id}")
    public void proposalPromise(String message) {
//        log.info("message proposalPromise {}",message);
        String[] messages = message.split(" ");
        String nId = messages[4];
        String word = messages[3];
        String lineNumber = messages[1];
        String totalLine = messages[0];
        String proposalNumber = messages[2];
        if(Role.PROPOSER.equals(role) && nodeId.equals((nId)) ){

            kafkaTemplate.send("proposal_accepted_topic",totalLine+" "+lineNumber+" "+  word +" "+proposalNumber+" "+nodeId);
        }

    }
    @KafkaListener(topics = "proposal_accepted_topic", groupId = "${spring.kafka.consumer.group-id}")
    public void acceptorAccepted(String message) {
        if(Role.ACCEPTOR.equals(role)){
            String[] messages = message.split(" ");
            String lineNumber = messages[1];
            String totalLine = messages[0];
            String word = messages[2];
            String proposalNumber = messages[3];
            String nId = messages[4];
            kafkaTemplate.send("learner_topic",totalLine+" "+lineNumber+" "+ proposalNumber + " " + word +" "+nId);
        }
    }
    @KafkaListener(topics = "learner_topic", groupId = "${spring.kafka.consumer.group-id}")
    public void countWord(String message) {
        if(Role.LEARNER.equals(role)){
            String[] messages = message.split(" ");
            String lineNumber = messages[1];
            String totalLine = messages[0];
            String word = messages[3];
//            log.info("word : {}",word);
//            log.info("totalAcceptors : {}",totalAcceptors);

//        Long lineNumber = Long.getLong(messages[3]);
//        Long wordCount = Long.getLong(messages[4]);
//        Long wCount = wordCountForLine.get(lineNumber);
//        if(Objects.isNull(wCount)){
//            wordCountForLine.put(lineNumber,wordCount);
//        }
            Integer count = wordCountForEachAcceptor.get(message);
            int newCount=0;
            //ToDo when recieve after majority of acceptor need to skipp
            if(Objects.isNull(count)){
                wordCountForEachAcceptor.put(message,1);
                newCount=1;
            }else{
                wordCountForEachAcceptor.put(message,count+1);
                newCount=count+1;
            }
//            log.info("newCount {} (totalAcceptors+1)/2) {}",newCount,(totalAcceptors+1)/2);
            if(newCount>=(totalAcceptors+1)/2){
//                log.info("word {} count {}",word,newCount);
                word=word.toUpperCase();
                ArrayList<String> strings = letterWordCount.get(word.charAt(0));
                char ch =word.charAt(0);
                if( (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')){

                    if(Objects.isNull(strings)){
                        ArrayList<String> words = new ArrayList<>();
                        words.add(word);

                        letterWordCount.put(word.charAt(0),words);
                    }else{
                        strings.add(word);
                        letterWordCount.put(word.charAt(0),strings);
                    }
                }

//                printOutput("a");
//            String word = messages[1];
//            ArrayList<String> wordList = tempWordList.get(lineNumber);
//            if(Objects.isNull(wordList)){
//                ArrayList<String> words = new ArrayList<>();
//                words.add(word);
//                tempWordList.put(lineNumber,words);
//            }else{
//                ArrayList<String> strings = tempWordList.get(lineNumber);
//                strings.add(word);
//                tempWordList.put(lineNumber,strings);
//            }

            }
            if(totalLine.equals(lineNumber)){
                kafkaTemplate.send("count_close","Completed");
            }
//            log.info("letterWordCount length {}",letterWordCount);
//        if(wordCountForLine.get(lineNumber)==tempWordList.get(lineNumber).size()){
//            saveWordInDatabase(lineNumber,tempWordList.get(lineNumber));
//        }
        }
    }
//    @Transactional
//    public void saveWordInDatabase(Long lineNumber,ArrayList<String> wordList){
//
//    }
@KafkaListener(topics = "count_close", groupId = "${spring.kafka.consumer.group-id}")
public void printOutput(String message) {
    if (Role.LEARNER.equals(role) && count == 1) {
        count--;
        System.out.println("-----------------------------------------------------------------------");
        System.out.printf("| %-5s | %-50s | %-5s |%n", "Letter", "Words", "Count");
        System.out.println("-----------------------------------------------------------------------");

        for (Map.Entry<Character, ArrayList<String>> entry : letterWordCount.entrySet()) {
            Character letter = entry.getKey();
            List<String> words = entry.getValue();
            int totalCount = words.size();

            StringBuilder currentLine = new StringBuilder();
            int currentWidth = 0;
            boolean isFirstLine = true;

            for (String word : words) {
                if (currentWidth + word.length() + 1 > 50) {  // If adding next word exceeds width
                    System.out.printf("| %-6s | %-50s | %-5s |%n",
                            isFirstLine ? letter : "",
                            currentLine.toString(),
                            isFirstLine ? totalCount : ""
                    );
                    currentLine = new StringBuilder();
                    currentWidth = 0;
                    isFirstLine = false;
                }

                if (currentLine.length() > 0) {
                    currentLine.append(",");
                    currentWidth++;
                }

                currentLine.append(word);
                currentWidth += word.length();
            }

            // Print last line for this letter
            if (currentLine.length() > 0) {
                System.out.printf("| %-6s | %-50s | %-5s |%n",
                        isFirstLine ? letter : "",
                        currentLine.toString(),
                        isFirstLine ? totalCount : ""
                );
            }
            System.out.println("-----------------------------------------------------------------------");
        }
//        System.out.println("-----------------------------------------------------------------------");
    }
}


    //    @KafkaListener(topics = "count_close", groupId = "${spring.kafka.consumer.group-id}")
//    public void printOutput(String message) {
//        if(Role.LEARNER.equals(role) && count==1){
//            count--;
//            for (Map.Entry<Character, ArrayList<String>> entry : letterWordCount.entrySet()) {
//                Character letter = entry.getKey();
//                ArrayList<String> words = entry.getValue();
//
//                String commaSeparatedWords = String.join(",", words);
//                int wordCount = words.size();
//
//                System.out.printf("%-10s %-50s %d%n", letter, commaSeparatedWords, wordCount);
//            }
//        }
//    }
    @KafkaListener(topics = "send_word_to_acceptor_node_topic", groupId = "${spring.kafka.consumer.group-id}")
    public void acceptWord(String message) {
        if(Role.ACCEPTOR.equals(role)){
            String[] messages = message.split(" ");
            String lineNumber = messages[1];
            String totalLine = messages[0];
            String word = messages[2];
            String nId = messages[4];

            int proposalNumber = Integer.parseInt(messages[3]);
            log.info("Working acceptWord {} {}",proposalNumber,highestProposalNumber);
            if (proposalNumber > highestProposalNumber) {
                highestProposalNumber = proposalNumber;
                log.info("Working acceptWord 1 acceptWord {}",message);
                kafkaTemplate.send("proposal_promise_topic", totalLine+" "+lineNumber+" "+ proposalNumber + " " + word +" "+nId);
            }
        }
    }

//    @KafkaListener(topics = "accepted_topic", groupId = "paxos-group")
//    public void learnAcceptedWord(String message) {
//        String[] parts = message.split(" ");
//        int proposalNumber = Integer.parseInt(parts[1]);
//        proposalMap.put(proposalNumber, proposalMap.get(proposalNumber) + 1);
//        if(){
//            kafkaTemplate.send("learner_topic",Role.Learner + proposalNumber + " " + word);
//        }
//    }
//    @KafkaListener(topics = "learner_topic", groupId = "paxos-group")
//    public void learnAcceptedWord(String message) {
//        String[] parts = message.split(" ");
//        String word = parts[2];
//        char startingLetter = parts[2].charAt(0);
//
//        wordCounts.computeIfAbsent(startingLetter, k -> new ArrayList<>()).add(word);
//
//        displayResults();
//    }
@KafkaListener(topics = "heartbeat_of_leader_topic", groupId = "${spring.kafka.consumer.group-id}")
public void onHeartbeat(String message) {
    if (role == Role.COORDINATOR) {
        for(String node:registeredNodes){
            if(node.equals(message)){
                lastHearBeatTimeMap.put(node,new Date());
            }
        }
    }
}
    @KafkaListener(topics = "heartbeat_of_other_node_topic", groupId = "${spring.kafka.consumer.group-id}")
    public void onHeartbeatOfOtherNode(String message) {
        if (message.equals(nodeId)) {
            lastSendTimeStamp= new Date();
        }
    }

    @Scheduled(fixedRate = 3000)
    public void checkLeaderHeartbeat() {
        if (role != Role.COORDINATOR && electedLeader != null) {
//            log.info("checkLeaderHeartbeat 1");
            if(Objects.nonNull(lastSendTimeStamp)){
                long diffInMillis = new Date().getTime() - lastSendTimeStamp.getTime();
                long diffInSeconds = diffInMillis / 1000;
                if(diffInSeconds>8){
                    electedLeader = null;
                    registeredNodesValues.remove(registeredNodes.indexOf(nodeId));
                    registeredNodes.remove(nodeId);
                    //TODO
//                    startElection();

                }else{
//                    log.info("Leader alive");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    kafkaTemplate.send("heartbeat_of_leader_topic", nodeId);
                }

            }else{
                kafkaTemplate.send("heartbeat_of_leader_topic", nodeId);
            }
        }
    }

    @Scheduled(fixedRate = 3000)
    public void checkOtherNodeHeartbeat() {
        if (role == Role.COORDINATOR && electedLeader != null) {
//            log.info("Check other heart beat");

            Iterator<Map.Entry<String, Date>> iterator = lastHearBeatTimeMap.entrySet().iterator();
            while (iterator.hasNext() && !iterator.equals(nodeId)) {
                Map.Entry<String, Date> entry = iterator.next();
                String nId = entry.getKey();
                if(!nId.equals(nodeId)){
                    Date lastTime = entry.getValue();

                    long timeDiff = new Date().getTime() - lastTime.getTime();
//                    log.info("Node ID: {} | Last Seen: {} | Time Diff: {}", nId, lastTime, timeDiff);

                    if (timeDiff > 8000) {
                        log.info("<<Node {} crashed>>", nId);
                        iterator.remove(); // Safe way to remove
                        kafkaTemplate.send("node_crash_topic", nId);

                        if (lastHearBeatTimeMap.size() < 4) {
                            log.info("<< Number of node count less than 4. >>");
                        } else {
                            // roleAssign(); // Uncomment when ready
                        }

                    } else {
//                        log.info("Node {} is alive", nId);
                        kafkaTemplate.send("heartbeat_of_other_node_topic",nId);
                        // Optional: update timestamp only if needed
                        // lastHearBeatTimeMap.put(nId, new Date());

                        try {
                            Thread.sleep(1000); // Optional, but maybe avoid in scheduled methods
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt(); // Best practice
                            log.error("Heartbeat check interrupted", e);
                        }
                    }
                }
            }
        }
    }

    @KafkaListener(topics = "node_crash_topic", groupId = "${spring.kafka.consumer.group-id}")
    public void onNodeCrash(String message) {
        log.info("<< Node crashed {} >>",message);
        registeredNodes.remove(message);
        if(registeredNodes.size()<4){
            log.info("<< Number of node count less than 4. >>");
        }
    }
}

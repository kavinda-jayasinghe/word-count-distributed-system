package lk.example.word_count_distributed_system;

import jakarta.annotation.PostConstruct;
import lk.example.word_count_distributed_system.service.impl.NodeRegistrationServiceImpl;
import lombok.RequiredArgsConstructor;
import lk.example.word_count_distributed_system.entity.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import lk.example.word_count_distributed_system.repository.NodeRepository;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@RequiredArgsConstructor
@EnableScheduling
public class WordCountDistributedSystemApplication {
	private static final Logger log = LoggerFactory.getLogger(WordCountDistributedSystemApplication.class);
	@Value("${server.port}")
	private  String port;
	@Value("${server.address}")
	private  String address;
//	@Value("${server.node.value}")
//	private  Long nodeValue;
	private final NodeRepository nodeRepository;
	private final NodeRegistrationServiceImpl nodeRegistrationServiceImpl;

	public static void main(String[] args) {
		SpringApplication.run(WordCountDistributedSystemApplication.class, args);

	}
	@PostConstruct
	private void init(){

//		save node

		Node newNode = new Node();
		newNode.setPort(port);
		newNode.setIpAddress(address);
//		newNode.setNodeValue(nodeValue);
//		nodeRepository.save(newNode);

	}
	private void startLeaderElection(){
//		List<Node> allActiveNode = nodeRepository.findAllByNodeStatus(NodeStatus.ACTIVE);
//		if(Objects.nonNull(allActiveNode) && allActiveNode.size()>=4){
//
//		}
	}
//
//	@Override
//	public void run(String... args) throws Exception {
//		log.info("Working run");
//		nodeRegistraionSeriveImpl.registerNode();
//	}
}

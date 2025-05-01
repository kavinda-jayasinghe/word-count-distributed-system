package lk.example.word_count_distributed_system.service.impl;

import lk.example.word_count_distributed_system.entity.Node;
import lk.example.word_count_distributed_system.repository.NodeRepository;
import lk.example.word_count_distributed_system.service.NodeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class NodeServiceImpl implements NodeService {
    private final NodeRegistrationServiceImpl nodeRegistrationServiceImpl;
    @Value("${server.port}")
    private  String port;
    private Boolean isCoordinator = Boolean.FALSE;
    private Node coordinatorNode=null;
    private final NodeRepository nodeRepository;
    @Override
    public void setCoordinator() {
//        isCoordinator=Boolean.TRUE;
//        setCoordinateNode(nodeRepository.findNodeByPort(port));

    }

    @Override
    public void updateCoordinator(Long nodeId) {
        setCoordinateNode(nodeRepository.findById(nodeId));

    }

    @Override
    public void start() {
        System.out.println("NodeServiceImpl");
        nodeRegistrationServiceImpl.registerNode();
    }

    @Override
    public void startAll() {
        nodeRegistrationServiceImpl.registerAll();
    }

    private void setCoordinateNode(Optional<Node> optionalNode){
        if(optionalNode.isPresent()){
            coordinatorNode=optionalNode.get();
            log.info("Coordinator node has been updated.");
        }else{
            log.error("Invalid node id {}",optionalNode);
        }
    }
}

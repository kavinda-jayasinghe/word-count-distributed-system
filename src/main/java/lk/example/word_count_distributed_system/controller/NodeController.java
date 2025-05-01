package lk.example.word_count_distributed_system.controller;

import lk.example.word_count_distributed_system.service.NodeService;
import lk.example.word_count_distributed_system.service.impl.NodeRegistrationListenerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/node")

public class NodeController {
    private final NodeService nodeService;
    private final NodeRegistrationListenerService nodeRegistrationListenerService;
    @GetMapping("/start")
    public void start(){
        System.out.println("start controller");
        nodeService.start();
        List<String> words = Arrays.asList("cat", "bat", "apple");

    }
    @GetMapping("/startAll")
    public void startAll(){
        nodeService.startAll();
    }
    @PostMapping("/set-coordinator")
    public void setCoordinator(){
        nodeService.setCoordinator();
    }
    @PostMapping("/configure")
    public void updateLeader(@RequestParam Long nodeId){
        nodeService.updateCoordinator(nodeId);
    }
}

package lk.example.word_count_distributed_system.repository;

import lk.example.word_count_distributed_system.entity.Node;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface NodeRepository extends JpaRepository<Node,Long> {
//    List<Node> findAllByNodeStatus(NodeStatus nodeStatus);
    Optional<Node> findNodeByPort(String port);
//    @Query(V)
//    List<Node> findOtherNode();
}

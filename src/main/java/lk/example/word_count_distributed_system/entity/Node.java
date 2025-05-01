package lk.example.word_count_distributed_system.entity;

import jakarta.persistence.*;
import lk.example.word_count_distributed_system.utils.enums.Role;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Entity
@Table(name = "node_details")
public class Node {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column
    private Long nodeValue;
//    @Column
//    @Enumerated(EnumType.STRING)
//    private NodeStatus nodeStatus;
    @Column
    private String ipAddress;
    @Column
    private String port;
    @Column
    @Enumerated(EnumType.STRING)
    private Role role;

}

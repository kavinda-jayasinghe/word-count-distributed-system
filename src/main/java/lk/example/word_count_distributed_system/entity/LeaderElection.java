package lk.example.word_count_distributed_system.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Entity
@Getter
@Setter
@Table(name = "leader_election")
public class LeaderElection {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(name = "node")
    private String node;
    @Column(name = "elected_date")
    private Date electedDate;
    @Column(name = "is_leader_crash")
    private Boolean isLeaderCrash;
    @Column(name = "port")
    private Long port;

}

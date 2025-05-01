package lk.example.word_count_distributed_system.repository;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import lk.example.word_count_distributed_system.entity.LeaderElection;
import org.springframework.data.jpa.repository.*;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface LeaderElectionRepository extends JpaRepository<LeaderElection,Long> {
//    @Query("select l from LeaderElection l where  l.isLeaderCrash=false")
//    Optional<LeaderElection> getExistLeader();
//    @Lock(LockModeType.PESSIMISTIC_WRITE)
//    @QueryHints({@QueryHint(name = "javax.persistence.lock.timeout", value = "2000")})

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({
            @QueryHint(name = "jakarta.persistence.lock.timeout", value = "2000") // ✅ Correct hint
    })
//    @Query("SELECT l FROM LeaderElection l WHERE l.isLeaderCrash = false")
    @Query("SELECT l FROM LeaderElection l WHERE l.isLeaderCrash = false")
    Optional<LeaderElection> getExistLeader();

    @Modifying
    @Query("DELETE FROM LeaderElection l WHERE l.isLeaderCrash = false")
    void deleteElectionStartNode();
}

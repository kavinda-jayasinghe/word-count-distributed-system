package lk.example.word_count_distributed_system.service;

public interface NodeService {
    public void setCoordinator();
    public void updateCoordinator(Long nodeId);
    public void start();
    public void startAll();
}

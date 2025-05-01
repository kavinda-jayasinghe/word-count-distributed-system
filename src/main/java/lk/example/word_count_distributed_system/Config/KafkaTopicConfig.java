package lk.example.word_count_distributed_system.Config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    @Bean
    public NewTopic nodeRegisterTopic(){
        return TopicBuilder
                .name("node_registration_topic")
                .build();
    }
    @Bean
    public NewTopic nodeElectionTopic(){
        return TopicBuilder
                .name("node_election_topic")
                .build();
    }@Bean
    public NewTopic nodeCoordinatorTopic(){
        return TopicBuilder
                .name("coordinator_topic")
                .build();
    }

}

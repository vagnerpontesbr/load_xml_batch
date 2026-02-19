package com.example.loadbatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.beans.factory.annotation.Value;

@SpringBootApplication
@EnableMongoRepositories
public class LoadBatchApplication {
    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(LoadBatchApplication.class, args)));
    }

    @Bean
    public BatchSummaryListener batchSummaryListener(
            @Value("${load-batch.summary-log:${APP_PATH:.}/summary.csv}") String path) {
        return new BatchSummaryListener(path);
    }
}

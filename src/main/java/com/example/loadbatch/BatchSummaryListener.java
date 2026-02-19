package com.example.loadbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class BatchSummaryListener implements JobExecutionListener {

    private static final Logger logger = LoggerFactory.getLogger(BatchSummaryListener.class);
    private final String summaryPath;

    public BatchSummaryListener(String summaryPath) {
        this.summaryPath = summaryPath;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        long readCount = jobExecution.getStepExecutions().stream().mapToLong(s -> s.getReadCount()).sum();
        long writeCount = jobExecution.getStepExecutions().stream().mapToLong(s -> s.getWriteCount()).sum();
        long skipCount = jobExecution.getStepExecutions().stream().mapToLong(s -> s.getSkipCount()).sum();

        Path path = Path.of(summaryPath);
        try (FileWriter fw = new FileWriter(summaryPath, true)) {
            if (Files.notExists(path) || Files.size(path) == 0) {
                fw.write("timestamp,read,write,skip,status\n");
            }
            fw.write(Instant.now() + "," + readCount + "," + writeCount + "," + skipCount + "," + jobExecution.getStatus() + "\n");
        } catch (IOException e) {
            logger.error("Failed to write summary CSV", e);
            return;
        }

        printLastThreeExecutions(path);
    }

    private void printLastThreeExecutions(Path path) {
        try {
            List<String> rows = Files.readAllLines(path).stream()
                .skip(1)
                .filter(line -> !line.isBlank())
                .collect(Collectors.toList());

            int start = Math.max(0, rows.size() - 3);
            logger.info("Last {} execution metrics from {}", rows.size() - start, summaryPath);
            logger.info("timestamp,read,write,skip,status");
            for (int i = start; i < rows.size(); i++) {
                logger.info(rows.get(i));
            }
        } catch (IOException e) {
            logger.error("Failed to read summary CSV for metrics display", e);
        }
    }
}

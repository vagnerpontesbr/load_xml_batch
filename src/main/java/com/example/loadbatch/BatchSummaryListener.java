package com.example.loadbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;

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

        try (FileWriter fw = new FileWriter(summaryPath, true)) {
            fw.write("timestamp,read,write,skip,status\n");
            fw.write(Instant.now() + "," + readCount + "," + writeCount + "," + skipCount + "," + jobExecution.getStatus() + "\n");
        } catch (IOException e) {
            logger.error("Failed to write summary CSV", e);
        }
    }
}

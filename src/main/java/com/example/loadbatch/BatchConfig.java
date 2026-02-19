package com.example.loadbatch;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.File;
import java.nio.file.StandardCopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Configuration
public class BatchConfig {

    private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);

    private final MongoTemplate mongoTemplate;
    private final XmlMapper xmlMapper = new XmlMapper();

    public BatchConfig(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Value("${load-batch.input-dir}")
    private String inputDir;

    @Value("${load-batch.threads:4}")
    private int threads;

    @Value("${load-batch.chunk-size:200}")
    private int chunkSize;

    @Value("${load-batch.writer-batch-size:1000}")
    private int writerBatchSize;

    @Value("${load-batch.unacknowledged-writes:false}")
    private boolean unacknowledgedWrites;

    @Value("${load-batch.failed-dir:${APP_PATH:.}/failed_xml}")
    private String failedDir;

    @Value("${load-batch.error-log:${APP_PATH:.}/failed_xml/skip_list.csv}")
    private String errorLogPath;

    @Bean
    public MultiResourceItemReader<FilePayload> multiResourceItemReader() throws Exception {
        File dir = new File(inputDir);
        if (!dir.exists()) {
            throw new IllegalStateException("Input dir not found: " + inputDir);
        }
        Resource[] resources;
        try (Stream<Path> paths = Files.list(dir.toPath())) {
            resources = paths
                .filter(p -> p.toString().endsWith(".xml"))
                .map(p -> new FileSystemResource(p.toFile()))
                .toArray(Resource[]::new);
        }

        return new MultiResourceItemReaderBuilder<FilePayload>()
            .name("xmlReader")
            .resources(resources)
            .delegate(new WholeFileItemReader())
            .saveState(false)
            .build();
    }

    @Bean
    public ItemProcessor<FilePayload, InvoiceRecord> xmlToJsonProcessor() {
        return payload -> {
            try {
                long t0 = System.nanoTime();
                Map<?, ?> map = xmlMapper.readValue(payload.getContent(), Map.class);
                long ms = (System.nanoTime() - t0) / 1_000_000;
                InvoiceRecord record = new InvoiceRecord(payload.getFilename(), (Map<String, Object>) map);
                record.setProcessMs(ms);
                if (logger.isDebugEnabled()) {
                    logger.debug("Processed {} in {}ms", payload.getFilename(), ms);
                }
                return record;
            } catch (Exception e) {
                logger.error("Error converting XML for file {}", payload.getFilename(), e);
                throw e;
            }
        };
    }

    // writer privado (não @Bean) — recebe o metricsListener para registrar tempos de insert
    private ItemWriter<InvoiceRecord> buildJsonWriter(BatchMetricsListener metricsListener) {
        return items -> {
            File failDir = new File(failedDir);
            failDir.mkdirs();

            MongoCollection<Document> collection = mongoTemplate.getCollection("invoices");
            if (unacknowledgedWrites) {
                collection = collection.withWriteConcern(WriteConcern.UNACKNOWLEDGED);
            }

            List<WriteModel<Document>> writes = new ArrayList<>(Math.min(items.size(), writerBatchSize));
            BulkWriteOptions options = new BulkWriteOptions().ordered(false);

            for (InvoiceRecord record : items) {
                Document doc = new Document(record.getData());
                doc.put("source_file", record.getFilename());
                writes.add(new InsertOneModel<>(doc));

                if (writes.size() >= writerBatchSize) {
                    flushBulkWrites(collection, writes, options, metricsListener);
                }
            }

            if (!writes.isEmpty()) {
                flushBulkWrites(collection, writes, options, metricsListener);
            }
        };
    }

    private void flushBulkWrites(
        MongoCollection<Document> collection,
        List<WriteModel<Document>> writes,
        BulkWriteOptions options,
        BatchMetricsListener metricsListener
    ) {
        long batchStart = System.nanoTime();
        try {
            collection.bulkWrite(writes, options);
            long batchMs = (System.nanoTime() - batchStart) / 1_000_000;
            metricsListener.recordWriteTime(batchMs);
            if (logger.isDebugEnabled()) {
                logger.debug("Inserted chunk size {} in {}ms", writes.size(), batchMs);
            }
            writes.clear();
        } catch (Exception batchEx) {
            logger.warn("Bulk insert failed for {} items, fallback item-by-item", writes.size(), batchEx);
            for (WriteModel<Document> write : writes) {
                InsertOneModel<Document> insert = (InsertOneModel<Document>) write;
                String filename = String.valueOf(insert.getDocument().get("source_file"));
                try {
                    long t0 = System.nanoTime();
                    collection.insertOne(insert.getDocument());
                    long ms = (System.nanoTime() - t0) / 1_000_000;
                    metricsListener.recordWriteTime(ms);
                } catch (Exception e) {
                    logger.error("Failed to write invoice {}", filename, e);
                    try {
                        File src = new File(inputDir, filename);
                        File dest = new File(failedDir, filename);
                        if (src.exists()) {
                            Files.move(src.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
                            logger.warn("Moved failed file {} to {}", filename, failedDir);
                        }
                    } catch (Exception moveEx) {
                        logger.error("Failed to move file {} to failed dir", filename, moveEx);
                    }
                }
            }
            writes.clear();
        }
    }

    @Bean
    public Step importStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws Exception {
        BatchMetricsListener metricsListener = new BatchMetricsListener();
        BatchErrorHandler errorHandler = new BatchErrorHandler(errorLogPath);
        ItemWriter<InvoiceRecord> writer = buildJsonWriter(metricsListener);
        return new StepBuilder("importStep", jobRepository)
            .<FilePayload, InvoiceRecord>chunk(chunkSize, transactionManager)
            .reader(multiResourceItemReader())
            .processor(xmlToJsonProcessor())
            .writer(writer)
            .listener((StepExecutionListener) metricsListener)
            .listener((ItemProcessListener<FilePayload, InvoiceRecord>) metricsListener)
            .listener((ItemWriteListener<InvoiceRecord>) metricsListener)
            .listener((ItemProcessListener<FilePayload, InvoiceRecord>) errorHandler)
            .listener((ItemWriteListener<InvoiceRecord>) errorHandler)
            .faultTolerant()
            .skip(Exception.class)
            .skipLimit(100)
            .taskExecutor(taskExecutor())
            .build();
    }

    @Bean
    public Job importJob(JobRepository jobRepository, Step importStep, BatchSummaryListener batchSummaryListener) {
        return new JobBuilder("importJob", jobRepository)
            .start(importStep)
            .listener(batchSummaryListener)
            .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threads);
        executor.setMaxPoolSize(threads);
        executor.setQueueCapacity(threads * 4);
        executor.setThreadNamePrefix("batch-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;
    }
}

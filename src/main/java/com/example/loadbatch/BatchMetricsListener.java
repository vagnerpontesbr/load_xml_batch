package com.example.loadbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.Chunk;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class BatchMetricsListener implements StepExecutionListener, ItemProcessListener<FilePayload, InvoiceRecord>, ItemWriteListener<InvoiceRecord> {

    private static final Logger logger = LoggerFactory.getLogger(BatchMetricsListener.class);

    // contadores simples (thread-safe)
    private final LongAdder processed = new LongAdder();
    private final LongAdder written   = new LongAdder();
    private final LongAdder failed    = new LongAdder();

    // métricas XML→JSON (thread-safe)
    private final LongAdder  processTotalMs = new LongAdder();
    private final LongAdder  processCount   = new LongAdder();
    private final AtomicLong processMinMs   = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong processMaxMs   = new AtomicLong(0);

    // métricas insert MongoDB (thread-safe)
    private final LongAdder  writeTotalMs   = new LongAdder();
    private final LongAdder  writeCount     = new LongAdder();
    private final AtomicLong writeMinMs     = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong writeMaxMs     = new AtomicLong(0);

    // ── helpers CAS para min/max ─────────────────────────────────────────────
    private static void updateMin(AtomicLong ref, long val) {
        long cur;
        do { cur = ref.get(); } while (val < cur && !ref.compareAndSet(cur, val));
    }

    private static void updateMax(AtomicLong ref, long val) {
        long cur;
        do { cur = ref.get(); } while (val > cur && !ref.compareAndSet(cur, val));
    }

    // ── StepExecutionListener ────────────────────────────────────────────────
    @Override
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Starting step {}", stepExecution.getStepName());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        long pCount = processCount.sum();
        long wCount = writeCount.sum();

        long pAvg = pCount > 0 ? processTotalMs.sum() / pCount : 0;
        long pMin = pCount > 0 ? processMinMs.get() : 0;
        long pMax = processMaxMs.get();

        long wAvg = wCount > 0 ? writeTotalMs.sum() / wCount : 0;
        long wMin = wCount > 0 ? writeMinMs.get() : 0;
        long wMax = writeMaxMs.get();

        logger.info("Step completed. processed={} written={} failed={}",
                processed.sum(), written.sum(), failed.sum());
        logger.info("  XML->JSON : min={}ms  avg={}ms  max={}ms  (n={})",
                pMin, pAvg, pMax, pCount);
        logger.info("  Insert    : min={}ms  avg={}ms  max={}ms  (n={})",
                wMin, wAvg, wMax, wCount);

        return stepExecution.getExitStatus();
    }

    // ── ItemProcessListener ──────────────────────────────────────────────────
    @Override
    public void afterProcess(FilePayload item, InvoiceRecord result) {
        processed.increment();
        long ms = result.getProcessMs();
        processTotalMs.add(ms);
        processCount.increment();
        updateMin(processMinMs, ms);
        updateMax(processMaxMs, ms);
    }

    @Override
    public void onProcessError(FilePayload item, Exception e) {
        failed.increment();
        logger.error("Process error for file {}", item.getFilename(), e);
    }

    // ── ItemWriteListener ────────────────────────────────────────────────────
    @Override
    public void afterWrite(Chunk<? extends InvoiceRecord> items) {
        written.add(items.size());
    }

    @Override
    public void onWriteError(Exception exception, Chunk<? extends InvoiceRecord> items) {
        failed.add(items.size());
        logger.error("Write error for batch of size {}", items.size(), exception);
    }

    // ── chamado diretamente pelo jsonWriter ──────────────────────────────────
    public void recordWriteTime(long ms) {
        writeTotalMs.add(ms);
        writeCount.increment();
        updateMin(writeMinMs, ms);
        updateMax(writeMaxMs, ms);
    }
}

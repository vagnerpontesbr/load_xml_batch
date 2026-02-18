package com.example.loadbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import java.io.FileWriter;
import java.io.IOException;

public class BatchErrorHandler implements ItemProcessListener<FilePayload, InvoiceRecord>, ItemWriteListener<InvoiceRecord> {

    private static final Logger logger = LoggerFactory.getLogger(BatchErrorHandler.class);
    private final String errorLogPath;

    public BatchErrorHandler(String errorLogPath) {
        this.errorLogPath = errorLogPath;
    }

    @Override
    public void onProcessError(FilePayload item, Exception e) {
        logError(item.getFilename(), "PROCESS", e.getMessage());
    }

    @Override
    public void onWriteError(Exception exception, Chunk<? extends InvoiceRecord> items) {
        for (InvoiceRecord rec : items) {
            logError(rec.getFilename(), "WRITE", exception.getMessage());
        }
    }

    private void logError(String filename, String stage, String message) {
        try (FileWriter fw = new FileWriter(errorLogPath, true)) {
            fw.write(filename + "," + stage + "," + message.replace(',', ';') + "\n");
        } catch (IOException e) {
            logger.error("Failed to write error log", e);
        }
    }
}

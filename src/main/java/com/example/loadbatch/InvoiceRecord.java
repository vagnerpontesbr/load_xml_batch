package com.example.loadbatch;

import java.util.Map;

public class InvoiceRecord {
    private final String filename;
    private final Map<String, Object> data;
    private long processMs;

    public InvoiceRecord(String filename, Map<String, Object> data) {
        this.filename = filename;
        this.data = data;
    }

    public String getFilename() {
        return filename;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public long getProcessMs() {
        return processMs;
    }

    public void setProcessMs(long processMs) {
        this.processMs = processMs;
    }
}

package com.example.loadbatch;

import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;

@Document(collection = "invoices")
public class InvoiceDocument {
    private Map<String, Object> data;

    public InvoiceDocument() {}

    public InvoiceDocument(Map<String, Object> data) {
        this.data = data;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }
}

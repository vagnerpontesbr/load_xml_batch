package com.example.loadbatch;

public class FilePayload {
    private final String filename;
    private final byte[] content;

    public FilePayload(String filename, byte[] content) {
        this.filename = filename;
        this.content = content;
    }

    public String getFilename() {
        return filename;
    }

    public byte[] getContent() {
        return content;
    }
}

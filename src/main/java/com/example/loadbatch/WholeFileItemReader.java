package com.example.loadbatch;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.nio.file.Files;

public class WholeFileItemReader implements ItemStreamReader<FilePayload>, ResourceAwareItemReaderItemStream<FilePayload> {

    private Resource resource;
    private boolean read = false;

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
        this.read = false;
    }

    @Override
    public FilePayload read() throws Exception {
        if (resource == null || read) {
            return null;
        }
        read = true;
        try {
            byte[] content = Files.readAllBytes(resource.getFile().toPath());
            return new FilePayload(resource.getFilename(), content);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read file: " + resource.getFilename(), e);
        }
    }

    @Override
    public void open(ExecutionContext executionContext) {}

    @Override
    public void update(ExecutionContext executionContext) {}

    @Override
    public void close() {}
}

package com.github.jwoschitz.avro.count;

import com.github.jwoschitz.avro.file.CountableSkipDataFileStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class AvroCount {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroCount.class);

    private InputStream in;
    private String path;

    private static class BufferedAvroInputStream extends BufferedInputStream {
        private final String path;

        BufferedAvroInputStream(InputStream in, String path) {
            super(in);
            this.path = path;
        }

        String getPath() {
            return path;
        }
    }

    public AvroCount(InputStream in) {
        this.in = in;
    }

    public AvroCount(String path) {
        this.path = path;
    }

    public long count() throws IOException {
        BufferedAvroInputStream stream;
        if (in != null) {
            stream = new BufferedAvroInputStream(in, "InputStream");
        } else {
            stream = openFromFS(path);
        }

        try {
            LOGGER.debug("Started to process {}", stream.getPath());

            long count = 0L;

            long startedProcessingAt = System.currentTimeMillis();
            try (CountableSkipDataFileStream streamReader = new CountableSkipDataFileStream(stream)) {
                while (streamReader.hasNextBlock()) {
                    streamReader.nextBlock();
                    count += streamReader.getBlockCount();
                }
            }

            LOGGER.debug("Processed {} in {}ms", stream.getPath(), System.currentTimeMillis() - startedProcessingAt);
            return count;
        } catch (Exception e) {
            LOGGER.error(String.format("Error occurred while processing %s", stream.getPath()), e);
            throw e;
        }
    }

    private static BufferedAvroInputStream openFromFS(String path) throws IOException {
        Path p = new File(path).toPath();
        if (Files.isRegularFile(p) && p.toString().endsWith(".avro")) {
            return new BufferedAvroInputStream(Files.newInputStream(p), p.toString());
        }

        throw new IOException("Provided path\"" + path + "\" must be a file that ends with \".avro\".");
    }
}

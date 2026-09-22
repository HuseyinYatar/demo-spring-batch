package com.batch.demo.batch.reject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.springframework.stereotype.Component;

import com.batch.demo.config.BatchProperties;

/**
 * Appends rejected CSV rows to an audit file. {@link #reset()} truncates the file and
 * writes a fresh header; it is called once per job run (see the job's beforeJob
 * listener) so each run's rejects file reflects only that run.
 */
@Component
public class CsvRejectedRecordSink implements RejectedRecordSink {

    private static final String HEADER = "stage,reason,timestamp,rawContent";

    private final Path path;

    public CsvRejectedRecordSink(BatchProperties properties) {
        this.path = Path.of(properties.getRejectsFilePath());
    }

    @Override
    public synchronized void reset() {
        try {
            Files.writeString(path, HEADER + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to initialize rejects file at " + path, e);
        }
    }

    @Override
    public synchronized void accept(RejectedRecord rejectedRecord) {
        String line = String.join(",",
                csvEscape(rejectedRecord.stage()),
                csvEscape(rejectedRecord.reason()),
                csvEscape(rejectedRecord.timestamp().toString()),
                csvEscape(rejectedRecord.rawContent()));
        try {
            Files.writeString(path, line + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to append to rejects file at " + path, e);
        }
    }

    private String csvEscape(String value) {
        if (value == null) {
            return "";
        }
        String escaped = value.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }
}

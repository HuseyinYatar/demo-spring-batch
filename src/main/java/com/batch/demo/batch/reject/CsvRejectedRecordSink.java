package com.batch.demo.batch.reject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;

import org.springframework.stereotype.Component;

import com.batch.demo.batch.support.BusinessDatePaths;
import com.batch.demo.config.BatchProperties;

/**
 * Appends rejected CSV rows to an audit file. {@link #reset()} truncates the file and
 * writes a fresh header; it is called once per fresh JobInstance (see the job's
 * beforeJob listener), never on a restart, so a restart's rejects accumulate onto
 * whatever the failed attempt already logged instead of losing it.
 */
@Component
public class CsvRejectedRecordSink implements RejectedRecordSink {

    private static final String HEADER = "stage,reason,timestamp,rawContent";

    private final String basePath;

    public CsvRejectedRecordSink(BatchProperties properties) {
        this.basePath = properties.getRejectsFilePath();
    }

    private Path pathFor(LocalDate businessDate) {
        return Path.of(BusinessDatePaths.withBusinessDate(basePath, businessDate));
    }

    @Override
    public synchronized void reset(LocalDate businessDate) {
        Path path = pathFor(businessDate);
        try {
            Files.writeString(path, HEADER + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to initialize rejects file at " + path, e);
        }
    }

    @Override
    public synchronized void accept(LocalDate businessDate, RejectedRecord rejectedRecord) {
        Path path = pathFor(businessDate);
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

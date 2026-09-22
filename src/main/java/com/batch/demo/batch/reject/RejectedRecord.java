package com.batch.demo.batch.reject;

import java.time.Instant;

public record RejectedRecord(String stage, String rawContent, String reason, Instant timestamp) {
}

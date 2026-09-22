package com.batch.demo.web.dto;

import java.time.LocalDateTime;

public record JobLaunchResponse(Long jobExecutionId, String status, LocalDateTime startTime) {
}

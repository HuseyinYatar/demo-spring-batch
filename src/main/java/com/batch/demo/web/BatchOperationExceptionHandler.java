package com.batch.demo.web;

import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.batch.demo.web.dto.ErrorResponse;

/**
 * Translates JobOperator's checked exceptions into HTTP status codes, kept separate
 * from BatchJobController so the controller stays a thin delegate.
 */
@RestControllerAdvice
public class BatchOperationExceptionHandler {

    /**
     * EmptyResultDataAccessException is what the JDBC-backed JobOperator actually
     * throws for an unknown execution id (confirmed by testing, not documented) -
     * its lookup uses jdbcTemplate.queryForObject before it ever gets a chance to
     * throw the checked NoSuchJobExecutionException the interface declares.
     */
    @ExceptionHandler({NoSuchJobExecutionException.class, NoSuchJobException.class, EmptyResultDataAccessException.class})
    public ResponseEntity<ErrorResponse> handleNotFound(Exception ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorResponse(ex.getMessage()));
    }

    @ExceptionHandler({JobExecutionAlreadyRunningException.class, JobExecutionNotRunningException.class,
            JobInstanceAlreadyCompleteException.class, JobRestartException.class, InvalidJobParametersException.class})
    public ResponseEntity<ErrorResponse> handleConflict(Exception ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponse(ex.getMessage()));
    }
}

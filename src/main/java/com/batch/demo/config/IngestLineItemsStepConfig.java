package com.batch.demo.config;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.database.JpaItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JpaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import org.hibernate.exception.JDBCConnectionException;

import jakarta.persistence.EntityManagerFactory;

import com.batch.demo.batch.dto.OrderLineCsvRecord;
import com.batch.demo.batch.listener.RejectedRecordSkipListener;
import com.batch.demo.batch.step1.IngestRetryListener;
import com.batch.demo.batch.step1.LineRangePartitioner;
import com.batch.demo.batch.step1.OrderLineFieldSetMapper;
import com.batch.demo.batch.step1.OrderLineItemValidationProcessor;
import com.batch.demo.batch.validation.InvalidOrderLineException;
import com.batch.demo.domain.OrderLineItemStaging;

@Configuration
public class IngestLineItemsStepConfig {

    @Bean
    @StepScope
    public FlatFileItemReader<OrderLineCsvRecord> orderLineItemReader(
            BatchProperties properties,
            ResourceLoader resourceLoader,
            OrderLineFieldSetMapper fieldSetMapper,
            @Value("#{stepExecutionContext['linesToSkip']}") Integer linesToSkip,
            @Value("#{stepExecutionContext['maxItemCount']}") Integer maxItemCount) {
        return new FlatFileItemReaderBuilder<OrderLineCsvRecord>()
                .name("orderLineItemReader")
                .resource(resourceLoader.getResource(properties.getInputCsvPath()))
                .linesToSkip(linesToSkip)
                .maxItemCount(maxItemCount)
                .delimited()
                .names("orderId", "customerId", "customerName", "productId", "productName",
                        "quantity", "unitPrice", "orderDate")
                .fieldSetMapper(fieldSetMapper)
                .build();
    }

    @Bean
    public JpaItemWriter<OrderLineItemStaging> orderLineItemStagingWriter(EntityManagerFactory entityManagerFactory) {
        return new JpaItemWriterBuilder<OrderLineItemStaging>()
                .entityManagerFactory(entityManagerFactory)
                .usePersist(true)
                .build();
    }

    @Bean
    public LineRangePartitioner lineRangePartitioner(BatchProperties properties, ResourceLoader resourceLoader) {
        return new LineRangePartitioner(properties, resourceLoader);
    }

    @Bean
    public Step ingestLineItemsWorkerStep(JobRepository jobRepository,
                                           PlatformTransactionManager transactionManager,
                                           BatchProperties properties,
                                           FlatFileItemReader<OrderLineCsvRecord> orderLineItemReader,
                                           ItemProcessor<OrderLineCsvRecord, OrderLineItemStaging> orderLineItemValidationProcessor,
                                           ItemWriter<OrderLineItemStaging> orderLineItemStagingWriter,
                                           RejectedRecordSkipListener rejectedRecordSkipListener,
                                           IngestRetryListener ingestRetryListener) {
        return new StepBuilder("ingestLineItemsWorkerStep", jobRepository)
                .<OrderLineCsvRecord, OrderLineItemStaging>chunk(properties.getChunkSize(), transactionManager)
                .reader(orderLineItemReader)
                .processor(orderLineItemValidationProcessor)
                .writer(orderLineItemStagingWriter)
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skip(InvalidOrderLineException.class)
                .skipLimit(properties.getSkipLimit())
                .listener(rejectedRecordSkipListener)
                // Real DB connection failures are retried, never skipped - same
                // reasoning as buildInvoicesStep: silently skipping a row because the
                // DB blipped would drop it with no audit trail, unlike the
                // FlatFileParseException/InvalidOrderLineException skips above, which
                // do have one via RejectedRecordSkipListener. Registered as Hibernate's
                // own JDBCConnectionException, not a Spring DataAccessException
                // subtype: orderLineItemStagingWriter is a plain JpaItemWriter using
                // EntityManager.persist() directly, which - unlike a Spring Data
                // repository call such as OrderPersistenceItemWriter's
                // orderRepository.saveAll() - never goes through Spring's persistence
                // exception translation, so the raw Hibernate exception is what
                // actually propagates here (confirmed via a real thrown-exception
                // stack trace, not assumed).
                .retry(JDBCConnectionException.class)
                .retryLimit(properties.getRetryLimit())
                .listener(ingestRetryListener)
                .build();
    }

    @Bean
    public Step ingestLineItemsStep(JobRepository jobRepository,
                                     BatchProperties properties,
                                     Step ingestLineItemsWorkerStep,
                                     LineRangePartitioner lineRangePartitioner,
                                     TaskExecutor batchTaskExecutor) {
        return new StepBuilder("ingestLineItemsStep", jobRepository)
                .partitioner("ingestLineItemsWorkerStep", lineRangePartitioner)
                .step(ingestLineItemsWorkerStep)
                .taskExecutor(batchTaskExecutor)
                .gridSize(properties.getPartitionGridSize())
                .build();
    }
}

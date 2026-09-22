package com.batch.demo.config;

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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import org.springframework.transaction.PlatformTransactionManager;

import jakarta.persistence.EntityManagerFactory;

import com.batch.demo.batch.dto.OrderLineCsvRecord;
import com.batch.demo.batch.listener.RejectedRecordSkipListener;
import com.batch.demo.batch.step1.OrderLineFieldSetMapper;
import com.batch.demo.batch.step1.OrderLineItemValidationProcessor;
import com.batch.demo.batch.validation.InvalidOrderLineException;
import com.batch.demo.domain.OrderLineItemStaging;

@Configuration
public class IngestLineItemsStepConfig {

    @Bean
    public FlatFileItemReader<OrderLineCsvRecord> orderLineItemReader(BatchProperties properties,
                                                                        ResourceLoader resourceLoader,
                                                                        OrderLineFieldSetMapper fieldSetMapper) {
        return new FlatFileItemReaderBuilder<OrderLineCsvRecord>()
                .name("orderLineItemReader")
                .resource(resourceLoader.getResource(properties.getInputCsvPath()))
                .linesToSkip(1)
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
    public Step ingestLineItemsStep(JobRepository jobRepository,
                                     PlatformTransactionManager transactionManager,
                                     BatchProperties properties,
                                     FlatFileItemReader<OrderLineCsvRecord> orderLineItemReader,
                                     ItemProcessor<OrderLineCsvRecord, OrderLineItemStaging> orderLineItemValidationProcessor,
                                     ItemWriter<OrderLineItemStaging> orderLineItemStagingWriter,
                                     RejectedRecordSkipListener rejectedRecordSkipListener) {
        return new StepBuilder("ingestLineItemsStep", jobRepository)
                .<OrderLineCsvRecord, OrderLineItemStaging>chunk(properties.getChunkSize(), transactionManager)
                .reader(orderLineItemReader)
                .processor(orderLineItemValidationProcessor)
                .writer(orderLineItemStagingWriter)
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skip(InvalidOrderLineException.class)
                .skipLimit(properties.getSkipLimit())
                .listener(rejectedRecordSkipListener)
                .build();
    }
}

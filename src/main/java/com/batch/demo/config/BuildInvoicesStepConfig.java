package com.batch.demo.config;

import java.util.List;

import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.infrastructure.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.demo.batch.dto.OrderInvoiceResult;
import com.batch.demo.batch.step2.DistinctOrderIdItemReader;
import com.batch.demo.batch.step2.InvoiceSummaryMergeTasklet;
import com.batch.demo.batch.step2.InvoiceSummaryFieldExtractor;
import com.batch.demo.batch.step2.InvoiceSummaryPartitionPaths;
import com.batch.demo.batch.step2.InvoiceWriteRetryListener;
import com.batch.demo.batch.step2.OrderIdRangePartitioner;
import com.batch.demo.batch.step2.OrderPersistenceItemWriter;
import com.batch.demo.batch.step2.StagingMarkProcessedItemWriter;
import com.batch.demo.batch.step2.TransientInvoiceWriteException;
import com.batch.demo.repository.OrderLineItemStagingRepository;

@Configuration
public class BuildInvoicesStepConfig {

    @Bean
    @StepScope
    public DistinctOrderIdItemReader distinctOrderIdItemReader(
            OrderLineItemStagingRepository stagingRepository,
            BatchProperties properties,
            @Value("#{stepExecutionContext['fromOrderId']}") String fromOrderId,
            @Value("#{stepExecutionContext['toOrderId']}") String toOrderId) {
        return new DistinctOrderIdItemReader(stagingRepository, fromOrderId, toOrderId, properties.getOrderIdPageSize());
    }

    /**
     * Step-scoped and suffixed per partition: a FlatFileItemWriter's open()/close()
     * lifecycle runs once per StepExecution, so a shared singleton here would have
     * concurrent partitions calling open() on the same instance and corrupting it.
     * Each partition writes its own invoice-summary-partitionN.csv instead.
     */
    @Bean
    @StepScope
    public FlatFileItemWriter<OrderInvoiceResult> invoiceSummaryCsvItemWriter(
            BatchProperties properties,
            InvoiceSummaryFieldExtractor fieldExtractor,
            @Value("#{stepExecutionContext['partitionKey']}") String partitionKey) {
        return new FlatFileItemWriterBuilder<OrderInvoiceResult>()
                .name("invoiceSummaryCsvItemWriter")
                .resource(new FileSystemResource(
                        InvoiceSummaryPartitionPaths.partitionPath(properties.getInvoiceSummaryOutputPath(), partitionKey)))
                .delimited()
                .delimiter(",")
                .fieldExtractor(fieldExtractor)
                .headerCallback(writer -> writer.write(
                        "orderNumber,customerName,invoiceNumber,subtotal,taxAmount,totalAmount,issuedDate"))
                .build();
    }

    @Bean
    public ItemWriter<OrderInvoiceResult> invoiceCompositeItemWriter(OrderPersistenceItemWriter orderPersistenceItemWriter,
                                                                       FlatFileItemWriter<OrderInvoiceResult> invoiceSummaryCsvItemWriter,
                                                                       StagingMarkProcessedItemWriter stagingMarkProcessedItemWriter) {
        CompositeItemWriter<OrderInvoiceResult> compositeItemWriter = new CompositeItemWriter<>();
        compositeItemWriter.setDelegates(List.of(
                orderPersistenceItemWriter,
                invoiceSummaryCsvItemWriter,
                stagingMarkProcessedItemWriter));
        return compositeItemWriter;
    }

    @Bean
    public OrderIdRangePartitioner orderIdRangePartitioner(OrderLineItemStagingRepository stagingRepository) {
        return new OrderIdRangePartitioner(stagingRepository);
    }

    @Bean
    public Step buildInvoicesWorkerStep(JobRepository jobRepository,
                                         PlatformTransactionManager transactionManager,
                                         BatchProperties properties,
                                         DistinctOrderIdItemReader distinctOrderIdItemReader,
                                         ItemProcessor<String, OrderInvoiceResult> invoiceAggregationProcessor,
                                         ItemWriter<OrderInvoiceResult> invoiceCompositeItemWriter,
                                         InvoiceWriteRetryListener invoiceWriteRetryListener) {
        return new StepBuilder("buildInvoicesWorkerStep", jobRepository)
                .<String, OrderInvoiceResult>chunk(properties.getChunkSize(), transactionManager)
                .reader(distinctOrderIdItemReader)
                .processor(invoiceAggregationProcessor)
                .writer(invoiceCompositeItemWriter)
                .faultTolerant()
                .retry(TransientInvoiceWriteException.class)
                // Real DB failures get the same retry treatment as the simulated one -
                // see IngestLineItemsStepConfig for why neither is registered as
                // skippable.
                .retry(TransientDataAccessException.class)
                .retry(DataAccessResourceFailureException.class)
                .retryLimit(properties.getRetryLimit())
                .listener(invoiceWriteRetryListener)
                .build();
    }

    @Bean
    public Step buildInvoicesStep(JobRepository jobRepository,
                                   BatchProperties properties,
                                   Step buildInvoicesWorkerStep,
                                   OrderIdRangePartitioner orderIdRangePartitioner,
                                   TaskExecutor batchTaskExecutor) {
        return new StepBuilder("buildInvoicesStep", jobRepository)
                .partitioner("buildInvoicesWorkerStep", orderIdRangePartitioner)
                .step(buildInvoicesWorkerStep)
                .taskExecutor(batchTaskExecutor)
                .gridSize(properties.getPartitionGridSize())
                .build();
    }

    @Bean
    public InvoiceSummaryMergeTasklet invoiceSummaryMergeTasklet(BatchProperties properties) {
        return new InvoiceSummaryMergeTasklet(properties);
    }

    @Bean
    public Step mergeInvoiceSummaryStep(JobRepository jobRepository,
                                         PlatformTransactionManager transactionManager,
                                         InvoiceSummaryMergeTasklet invoiceSummaryMergeTasklet) {
        return new StepBuilder("mergeInvoiceSummaryStep", jobRepository)
                .tasklet(invoiceSummaryMergeTasklet, transactionManager)
                .build();
    }
}

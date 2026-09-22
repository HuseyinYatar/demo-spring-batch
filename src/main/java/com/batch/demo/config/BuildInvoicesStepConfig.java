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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.demo.batch.dto.OrderInvoiceResult;
import com.batch.demo.batch.step2.DistinctOrderIdItemReader;
import com.batch.demo.batch.step2.InvoiceSummaryFieldExtractor;
import com.batch.demo.batch.step2.InvoiceWriteRetryListener;
import com.batch.demo.batch.step2.OrderPersistenceItemWriter;
import com.batch.demo.batch.step2.StagingMarkProcessedItemWriter;
import com.batch.demo.batch.step2.TransientInvoiceWriteException;
import com.batch.demo.repository.OrderLineItemStagingRepository;

@Configuration
public class BuildInvoicesStepConfig {

    @Bean
    @StepScope
    public DistinctOrderIdItemReader distinctOrderIdItemReader(OrderLineItemStagingRepository stagingRepository) {
        return new DistinctOrderIdItemReader(stagingRepository);
    }

    @Bean
    public FlatFileItemWriter<OrderInvoiceResult> invoiceSummaryCsvItemWriter(BatchProperties properties,
                                                                                InvoiceSummaryFieldExtractor fieldExtractor) {
        return new FlatFileItemWriterBuilder<OrderInvoiceResult>()
                .name("invoiceSummaryCsvItemWriter")
                .resource(new FileSystemResource(properties.getInvoiceSummaryOutputPath()))
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
    public Step buildInvoicesStep(JobRepository jobRepository,
                                   PlatformTransactionManager transactionManager,
                                   BatchProperties properties,
                                   DistinctOrderIdItemReader distinctOrderIdItemReader,
                                   ItemProcessor<String, OrderInvoiceResult> invoiceAggregationProcessor,
                                   ItemWriter<OrderInvoiceResult> invoiceCompositeItemWriter,
                                   InvoiceWriteRetryListener invoiceWriteRetryListener) {
        return new StepBuilder("buildInvoicesStep", jobRepository)
                .<String, OrderInvoiceResult>chunk(properties.getChunkSize(), transactionManager)
                .reader(distinctOrderIdItemReader)
                .processor(invoiceAggregationProcessor)
                .writer(invoiceCompositeItemWriter)
                .faultTolerant()
                .retry(TransientInvoiceWriteException.class)
                .retryLimit(properties.getRetryLimit())
                .skip(TransientInvoiceWriteException.class)
                .skipLimit(properties.getSkipLimit())
                .listener(invoiceWriteRetryListener)
                .build();
    }
}

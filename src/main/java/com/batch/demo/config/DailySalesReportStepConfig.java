package com.batch.demo.config;

import java.time.LocalDate;
import java.util.Map;

import jakarta.persistence.EntityManagerFactory;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.database.JpaPagingItemReader;
import org.springframework.batch.infrastructure.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.demo.batch.observability.ChunkTracingListener;
import com.batch.demo.batch.step3.DailyInvoiceDetailFieldExtractor;
import com.batch.demo.batch.step3.DailySalesReportPaths;
import com.batch.demo.batch.step3.DailySalesSummaryTasklet;
import com.batch.demo.domain.Invoice;
import com.batch.demo.repository.DailySalesReportRepository;
import com.batch.demo.repository.InvoiceRepository;

/**
 * Builds dailySalesReportJob: a chunk-oriented detail step (a new reader type for this
 * codebase, JpaPagingItemReader) followed by a plain Tasklet summary step. Deliberately
 * not partitioned, unlike steps 1/2 - a single day's invoice volume is a small slice of
 * the full historical dataset, so a single-threaded chunk step is the honest choice
 * here, not a missed opportunity.
 *
 * This job is only ever meant to run nested inside dailyPipelineJob (see
 * DailyPipelineJobConfig) - it has no REST endpoint of its own.
 */
@Configuration
public class DailySalesReportStepConfig {

    @Bean
    @StepScope
    public JpaPagingItemReader<Invoice> dailyInvoiceItemReader(
            EntityManagerFactory entityManagerFactory,
            BatchProperties properties,
            @Value("#{jobParameters['businessDate']}") LocalDate businessDate) {
        return new JpaPagingItemReaderBuilder<Invoice>()
                .name("dailyInvoiceItemReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select i from Invoice i where i.issuedDate = :issuedDate order by i.id")
                .parameterValues(Map.of("issuedDate", businessDate))
                .pageSize(properties.getChunkSize())
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<Invoice> dailyInvoiceDetailCsvItemWriter(
            BatchProperties properties,
            DailyInvoiceDetailFieldExtractor fieldExtractor,
            @Value("#{jobParameters['businessDate']}") LocalDate businessDate) {
        DailySalesReportPaths.ensureDirectoryExists(properties.getDailySalesReportOutputDir());
        return new FlatFileItemWriterBuilder<Invoice>()
                .name("dailyInvoiceDetailCsvItemWriter")
                .resource(new FileSystemResource(
                        DailySalesReportPaths.detailCsvPath(properties.getDailySalesReportOutputDir(), businessDate)))
                .delimited()
                .delimiter(",")
                .fieldExtractor(fieldExtractor)
                .headerCallback(writer -> writer.write(
                        "orderNumber,customerName,invoiceNumber,subtotal,taxAmount,totalAmount,issuedDate"))
                .build();
    }

    @Bean
    public Step dailyInvoiceDetailStep(JobRepository jobRepository,
                                        PlatformTransactionManager transactionManager,
                                        BatchProperties properties,
                                        JpaPagingItemReader<Invoice> dailyInvoiceItemReader,
                                        FlatFileItemWriter<Invoice> dailyInvoiceDetailCsvItemWriter,
                                        ChunkTracingListener chunkTracingListener) {
        return new StepBuilder("dailyInvoiceDetailStep", jobRepository)
                .<Invoice, Invoice>chunk(properties.getChunkSize(), transactionManager)
                .reader(dailyInvoiceItemReader)
                .writer(dailyInvoiceDetailCsvItemWriter)
                .listener(chunkTracingListener)
                .build();
    }

    @Bean
    public DailySalesSummaryTasklet dailySalesSummaryTasklet(InvoiceRepository invoiceRepository,
                                                               DailySalesReportRepository dailySalesReportRepository,
                                                               BatchProperties properties) {
        return new DailySalesSummaryTasklet(invoiceRepository, dailySalesReportRepository, properties);
    }

    @Bean
    public Step dailySalesSummaryStep(JobRepository jobRepository,
                                       PlatformTransactionManager transactionManager,
                                       DailySalesSummaryTasklet dailySalesSummaryTasklet) {
        return new StepBuilder("dailySalesSummaryStep", jobRepository)
                .tasklet(dailySalesSummaryTasklet, transactionManager)
                .build();
    }

    @Bean
    public Job dailySalesReportJob(JobRepository jobRepository,
                                    Step dailyInvoiceDetailStep,
                                    Step dailySalesSummaryStep) {
        return new JobBuilder("dailySalesReportJob", jobRepository)
                .start(dailyInvoiceDetailStep)
                .next(dailySalesSummaryStep)
                .build();
    }
}

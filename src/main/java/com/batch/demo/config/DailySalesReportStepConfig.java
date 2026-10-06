package com.batch.demo.config;

import java.time.LocalDate;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.database.JdbcPagingItemReader;
import org.springframework.batch.infrastructure.item.database.Order;
import org.springframework.batch.infrastructure.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.demo.batch.observability.ChunkTracingListener;
import com.batch.demo.batch.step3.AnalyzeTableTasklet;
import com.batch.demo.batch.step3.DailyInvoiceDetail;
import com.batch.demo.batch.step3.DailyInvoiceDetailFieldExtractor;
import com.batch.demo.batch.step3.DailySalesReportPaths;
import com.batch.demo.batch.step3.DailySalesSummaryTasklet;
import com.batch.demo.repository.DailySalesReportRepository;
import com.batch.demo.repository.InvoiceRepository;

/**
 * Builds dailySalesReportJob: a tasklet that analyzes the invoice table, a chunk-oriented
 * detail step (a new reader type for this codebase, JdbcPagingItemReader) and a plain
 * Tasklet summary step. Deliberately not partitioned, unlike steps 1/2 - a single day's
 * invoice volume is a small slice of the full historical dataset, so a single-threaded
 * chunk step is the honest choice here, not a missed opportunity.
 *
 * This job is only ever meant to run nested inside dailyPipelineJob (see
 * DailyPipelineJobConfig) - it has no REST endpoint of its own.
 */
@Configuration
public class DailySalesReportStepConfig {

    /**
     * Keyset-paged ({@code invoice_id > lastSeen}) JDBC read of the day's invoices as flat
     * {@link DailyInvoiceDetail} rows. This replaced a JpaPagingItemReader over Invoice entities,
     * which was slow for two separate reasons (71k invoices, measured):
     * <ul>
     *   <li>N+1: {@code Invoice.order} is an EAGER {@code @OneToOne}, so every invoice cost an
     *       extra SELECT for its Order - ~72k statements. Reading only the columns the CSV needs
     *       with an explicit join has no associations to resolve.</li>
     *   <li>OFFSET paging: page n re-scans and discards the first n*pageSize rows.</li>
     * </ul>
     *
     * The join is wrapped in a subquery on purpose: the paging reader looks the sort key up in
     * the ResultSet by its literal name, so a qualified {@code i.id} sort key fails ("column
     * i.id was not found"), and a bare {@code id} is ambiguous between invoice and orders.
     * The subquery gives it the plain alias {@code invoice_id}; Postgres flattens it, so the
     * predicates still reach the invoice table.
     *
     * The page size is its own property ({@code batch.report-page-size}), not the chunk size:
     * there are no managed entities here, so nothing ties a page to a chunk transaction. It
     * matters a lot right after the order job, when the freshly bulk-loaded invoice table has
     * no planner statistics yet (autovacuum's ANALYZE hasn't run): Postgres then estimates ~15
     * matching rows, picks "scan the table and sort" and pays ~40 ms per page, so the page
     * count decides the runtime - 24 s at 100 rows per page, 2.6 s at 1000, versus 0.6 s per
     * 71k rows once statistics exist.
     *
     * Returns the concrete reader class, not ItemReader: a step-scoped bean typed by the
     * interface never gets open()/close() called.
     */
    @Bean
    @StepScope
    public JdbcPagingItemReader<DailyInvoiceDetail> dailyInvoiceItemReader(
            DataSource dataSource,
            BatchProperties properties,
            @Value("#{jobParameters['businessDate']}") LocalDate businessDate) throws Exception {
        return new JdbcPagingItemReaderBuilder<DailyInvoiceDetail>()
                .name("dailyInvoiceItemReader")
                .dataSource(dataSource)
                .selectClause("invoice_id, order_number, customer_name, invoice_number, "
                        + "subtotal, tax_amount, total_amount, issued_date")
                .fromClause("(select i.id as invoice_id, o.order_number, o.customer_name, i.invoice_number, "
                        + "i.subtotal, i.tax_amount, i.total_amount, i.issued_date "
                        + "from invoice i join orders o on o.id = i.order_id) detail")
                .whereClause("issued_date = :issuedDate")
                .sortKeys(Map.of("invoice_id", Order.ASCENDING))
                .parameterValues(Map.of("issuedDate", businessDate))
                .pageSize(properties.getReportPageSize())
                .rowMapper((rs, rowNum) -> new DailyInvoiceDetail(
                        rs.getString("order_number"),
                        rs.getString("customer_name"),
                        rs.getString("invoice_number"),
                        rs.getBigDecimal("subtotal"),
                        rs.getBigDecimal("tax_amount"),
                        rs.getBigDecimal("total_amount"),
                        rs.getObject("issued_date", LocalDate.class)))
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<DailyInvoiceDetail> dailyInvoiceDetailCsvItemWriter(
            BatchProperties properties,
            DailyInvoiceDetailFieldExtractor fieldExtractor,
            @Value("#{jobParameters['businessDate']}") LocalDate businessDate) {
        DailySalesReportPaths.ensureDirectoryExists(properties.getDailySalesReportOutputDir());
        return new FlatFileItemWriterBuilder<DailyInvoiceDetail>()
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
                                        JdbcPagingItemReader<DailyInvoiceDetail> dailyInvoiceItemReader,
                                        FlatFileItemWriter<DailyInvoiceDetail> dailyInvoiceDetailCsvItemWriter,
                                        ChunkTracingListener chunkTracingListener) {
        return new StepBuilder("dailyInvoiceDetailStep", jobRepository)
                .<DailyInvoiceDetail, DailyInvoiceDetail>chunk(properties.getChunkSize(), transactionManager)
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

    /**
     * First step of the job: refreshes planner statistics for the invoices the order job has
     * just bulk-loaded, before the detail reader and the summary aggregates query them (see
     * AnalyzeTableTasklet). Only invoice needs it - orders is joined by primary key.
     */
    @Bean
    public Step analyzeInvoicesStep(JobRepository jobRepository,
                                    PlatformTransactionManager transactionManager,
                                    JdbcTemplate jdbcTemplate) {
        return new StepBuilder("analyzeInvoicesStep", jobRepository)
                .tasklet(new AnalyzeTableTasklet(jdbcTemplate, "invoice"), transactionManager)
                .build();
    }

    @Bean
    public Job dailySalesReportJob(JobRepository jobRepository,
                                    Step analyzeInvoicesStep,
                                    Step dailyInvoiceDetailStep,
                                    Step dailySalesSummaryStep) {
        return new JobBuilder("dailySalesReportJob", jobRepository)
                .start(analyzeInvoicesStep)
                .next(dailyInvoiceDetailStep)
                .next(dailySalesSummaryStep)
                .build();
    }
}

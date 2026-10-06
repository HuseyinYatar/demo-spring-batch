package com.batch.demo;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.database.JdbcPagingItemReader;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.batch.test.StepScopeTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import com.batch.demo.batch.step3.DailyInvoiceDetail;
import com.batch.demo.batch.step3.DailySalesReportPaths;
import com.batch.demo.config.BatchProperties;
import com.batch.demo.domain.Invoice;
import com.batch.demo.domain.Order;
import com.batch.demo.domain.OrderStatus;
import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The daily report's detail step used to read Invoice entities with a JpaPagingItemReader:
 * one extra SELECT per invoice (the eager Invoice.order) and OFFSET paging that re-scans
 * earlier pages. It now reads flat rows with a keyset-paged JdbcPagingItemReader, whose
 * paging and resume behaviour is different code - pinned here, with a page size (3) small
 * enough that every test crosses page boundaries.
 */
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = "batch.report-page-size=3")
class DailyInvoiceDetailReaderTest extends AbstractPostgresIntegrationTest {

    private static final LocalDate DAY = LocalDate.of(2099, 3, 1);
    private static final LocalDate OTHER_DAY = LocalDate.of(2099, 3, 2);

    @Autowired
    private JdbcPagingItemReader<DailyInvoiceDetail> reader;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("orderProcessingJob")
    private Job orderProcessingJob;

    @Autowired
    @Qualifier("dailySalesReportJob")
    private Job dailySalesReportJob;

    @Autowired
    private BatchProperties properties;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void readsEveryInvoiceOfTheDayExactlyOnceAcrossPagesInInvoiceOrder() throws Exception {
        seedInterleavedDays();

        List<DailyInvoiceDetail> rows = read(DAY, new ExecutionContext(), Integer.MAX_VALUE);

        // 7 invoices at 3 per page = pages of 3, 3, 1; the other day's are never returned.
        assertThat(rows).extracting(DailyInvoiceDetail::orderNumber)
                .containsExactly("ORD-D1", "ORD-D2", "ORD-D3", "ORD-D4", "ORD-D5", "ORD-D6", "ORD-D7");
        DailyInvoiceDetail first = rows.get(0);
        assertThat(first.customerName()).isEqualTo("Customer D1");
        assertThat(first.invoiceNumber()).isEqualTo("INV-ORD-D1");
        assertThat(first.subtotal()).isEqualByComparingTo("10.00");
        assertThat(first.taxAmount()).isEqualByComparingTo("1.80");
        assertThat(first.totalAmount()).isEqualByComparingTo("11.80");
        assertThat(first.issuedDate()).isEqualTo(DAY);
    }

    @Test
    void resumesRightAfterTheLastItemReadWhateverThePagePosition() throws Exception {
        seedInterleavedDays();
        List<String> all = read(DAY, new ExecutionContext(), Integer.MAX_VALUE).stream()
                .map(DailyInvoiceDetail::orderNumber).toList();

        // Positions inside a page (1, 4), exactly on page boundaries (0, 3, 6) and at the end (7):
        // a restarted step opens a new reader with the saved ExecutionContext and must neither
        // repeat nor skip an invoice.
        for (int readBeforeRestart : new int[] {0, 1, 3, 4, 6, 7}) {
            ExecutionContext saved = new ExecutionContext();
            List<String> before = read(DAY, saved, readBeforeRestart).stream()
                    .map(DailyInvoiceDetail::orderNumber).toList();
            List<String> after = read(DAY, saved, Integer.MAX_VALUE).stream()
                    .map(DailyInvoiceDetail::orderNumber).toList();

            List<String> combined = new ArrayList<>(before);
            combined.addAll(after);
            assertThat(before).as("read before restart (%d)", readBeforeRestart).hasSize(readBeforeRestart);
            assertThat(combined).as("before + after restart at %d", readBeforeRestart).isEqualTo(all);
        }
    }

    @Test
    void theDetailCsvHasOneLinePerInvoiceWithTheInvoicesOwnValues() throws Exception {
        assertThat(jobLauncher.run(orderProcessingJob, new JobParametersBuilder()
                .addLocalDate("businessDate", DAY)
                .addString("inputFile", "classpath:data/test-order-line-items.csv")
                .toJobParameters()).getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(jobLauncher.run(dailySalesReportJob, new JobParametersBuilder()
                .addLocalDate("businessDate", DAY)
                .addLong("runId", 1L)
                .toJobParameters()).getStatus()).isEqualTo(BatchStatus.COMPLETED);

        List<String> expected = new ArrayList<>();
        expected.add("orderNumber,customerName,invoiceNumber,subtotal,taxAmount,totalAmount,issuedDate");
        for (Map<String, Object> row : jdbcTemplate.queryForList(
                "select o.order_number, o.customer_name, i.invoice_number, i.subtotal, i.tax_amount, "
                        + "i.total_amount, i.issued_date from invoice i join orders o on o.id = i.order_id "
                        + "order by i.id")) {
            expected.add(String.join(",", row.get("order_number").toString(), row.get("customer_name").toString(),
                    row.get("invoice_number").toString(), row.get("subtotal").toString(),
                    row.get("tax_amount").toString(), row.get("total_amount").toString(),
                    row.get("issued_date").toString()));
        }

        // 6 invoices at 3 per page: the real step reads two pages.
        assertThat(expected).hasSize(7);
        assertThat(Files.readAllLines(DailySalesReportPaths.detailCsvPath(properties.getDailySalesReportOutputDir(), DAY)))
                .isEqualTo(expected);
    }

    /** Opens the step-scoped reader like a (re)started step would, reads up to {@code max} items, saves state. */
    private List<DailyInvoiceDetail> read(LocalDate businessDate, ExecutionContext executionContext, int max)
            throws Exception {
        StepExecution stepExecution = MetaDataInstanceFactory.createStepExecution(
                new JobParametersBuilder().addLocalDate("businessDate", businessDate).toJobParameters());
        return StepScopeTestUtils.doInStepScope(stepExecution, () -> {
            reader.open(executionContext);
            List<DailyInvoiceDetail> items = new ArrayList<>();
            for (int i = 0; i < max; i++) {
                DailyInvoiceDetail item = reader.read();
                if (item == null) {
                    break;
                }
                items.add(item);
            }
            reader.update(executionContext);
            reader.close();
            return items;
        });
    }

    /** 7 invoices for DAY and 3 for OTHER_DAY, saved alternately so their ids interleave. */
    private void seedInterleavedDays() {
        for (int i = 1; i <= 7; i++) {
            seedInvoice("ORD-D" + i, "Customer D" + i, DAY);
            if (i % 3 == 1) {
                seedInvoice("ORD-X" + i, "Customer X" + i, OTHER_DAY);
            }
        }
    }

    private void seedInvoice(String orderNumber, String customerName, LocalDate issuedDate) {
        BigDecimal subtotal = new BigDecimal("10.00");
        Order order = Order.builder()
                .orderNumber(orderNumber).customerId("C-1").customerName(customerName)
                .orderDate(issuedDate).status(OrderStatus.INVOICED).build();
        order.setInvoice(Invoice.builder()
                .order(order).invoiceNumber("INV-" + orderNumber)
                .subtotal(subtotal).taxAmount(new BigDecimal("1.80")).totalAmount(new BigDecimal("11.80"))
                .issuedDate(issuedDate).build());
        orderRepository.save(order);
    }
}

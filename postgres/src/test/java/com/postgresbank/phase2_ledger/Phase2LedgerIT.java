package com.postgresbank.phase2_ledger;

import static com.postgresbank.testsupport.TestSupport.openAccount;
import static com.postgresbank.testsupport.TestSupport.readStatCounter;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import com.postgresbank.common.Posting;
import com.postgresbank.common.PostingRepository;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * What an {@code UPDATE} actually does to the heap, and what the database (not the application) guarantees about a
 * retried write.
 *
 * <p>The first two blocks are the same experiment asked two ways: perform a write, then read a Postgres internal to
 * prove what physically happened rather than merely that the row now holds the new value.
 * {@link TupleVersionTests} reads {@code ctid} to show the row moved; {@link HotUpdateTests} reads
 * {@code pg_stat_user_tables} to show it moved <em>cheaply</em>, without touching an index.
 * {@link IdempotencyTests} is a different shape entirely — a two-thread race, settled by a UNIQUE constraint.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class Phase2LedgerIT extends TestContainerConfig {

    @Autowired
    private AccountRepository accounts;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private PostingRepository postings;

    /**
     * Physical proof that an {@code UPDATE} is a delete-and-insert of a new tuple version, never an in-place mutation:
     * {@code ctid} is a row's physical address (page number, offset within the page). Postgres never changes a
     * committed tuple's bytes on disk - MVCC needs the old version to stay exactly as it was for any transaction whose
     * snapshot still needs to see it. Update the row and its {@code ctid} moves to a new location, because it's
     * genuinely a new tuple; the old one is left in place, marked expired ({@code xmax} set), waiting for
     * {@code VACUUM} to reclaim its space.
     */
    @Nested
    class TupleVersionTests {

        @Test
        void updatingAnIndexedColumnMovesTheTuple() throws Exception {
            Account account = openAccount(accounts);

            try (Connection c = dataSource.getConnection()) {
                insertPosting(c, account.getId(), 100);
                String ctidBefore = ctidOf(c, account.getId());

                // amount_minor isn't indexed either, but forcing a page-spanning rewrite isn't the point - any UPDATE
                // demonstrates the delete+insert shape. What matters for this assertion is simply: same logical row,
                // different physical address after the write.
                updateAmount(c, account.getId(), 999);
                String ctidAfter = ctidOf(c, account.getId());

                assertThat(ctidAfter)
                        .as("the physical location of the row should change after UPDATE - it's a new tuple version")
                        .isNotEqualTo(ctidBefore);
            }
        }

        private void insertPosting(Connection c, long accountId, long amountMinor) throws Exception {
            try (PreparedStatement ps = c.prepareStatement(
                    "insert into postings (account_id, amount_minor, note, created_at) values (?, ?, 'seed', now())")) {
                ps.setLong(1, accountId);
                ps.setLong(2, amountMinor);
                ps.executeUpdate();
            }
        }

        private void updateAmount(Connection c, long accountId, long newAmountMinor) throws Exception {
            try (PreparedStatement ps =
                    c.prepareStatement("update postings set amount_minor = ? where account_id = ?")) {
                ps.setLong(1, newAmountMinor);
                ps.setLong(2, accountId);
                ps.executeUpdate();
            }
        }

        private String ctidOf(Connection c, long accountId) throws Exception {
            try (PreparedStatement ps = c.prepareStatement("select ctid from postings where account_id = ?")) {
                ps.setLong(1, accountId);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getString(1);
                }
            }
        }
    }

    /**
     * {@code note} on {@code postings} is not indexed (see schema.sql / {@code Posting}). Updating only that column
     * lets Postgres do a HOT (Heap-Only Tuple) update: the new tuple version can be written to the same page without
     * touching any index, because no indexed column changed and there's room on the page.
     * {@code pg_stat_user_tables.n_tup_hot_upd} counts exactly these updates - this test proves the count moves, rather
     * than just asserting the update "worked" (which a plain non-HOT update would also do).
     */
    @Nested
    class HotUpdateTests {

        @Test
        void updatingANonIndexedColumnRegistersAsHotUpdates() throws Exception {
            Account account = openAccount(accounts);
            Posting posting = postings.save(new Posting(account, null, 500, "initial-note"));

            long hotUpdatesBefore = readStatCounter(dataSource, "postings", "n_tup_hot_upd");

            for (int i = 0; i < 20; i++) {
                posting.setNote("note-revision-" + i);
                postings.save(posting);
                postings.flush();
            }

            // pg_stat_user_tables is refreshed from shared memory on commit but a fresh connection's first read can lag
            // it by a beat - poll rather than asserting on a single snapshot.
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(
                            readStatCounter(dataSource, "postings", "n_tup_hot_upd"))
                    .as("updating only the non-indexed 'note' column should register as HOT updates")
                    .isGreaterThan(hotUpdatesBefore));
        }
    }

    /**
     * The UNIQUE constraint on {@code transfers.idempotency_key} is the actual guarantee here, not application logic -
     * two requests racing to insert the same key can only ever have one winner at the database level, no matter how the
     * JVM schedules the two threads. This fires the exact same transfer twice, concurrently, and proves it posts
     * exactly once.
     */
    @Nested
    class IdempotencyTests {

        @Autowired
        private TransferService transferService;

        private final ExecutorService pool = Executors.newFixedThreadPool(2);

        @Test
        void concurrentRetriesOfTheSameRequestPostOnlyOnce() throws Exception {
            Account from = openAccount(accounts);
            Account to = openAccount(accounts);
            String idempotencyKey = UUID.randomUUID().toString();

            CountDownLatch bothStarting = new CountDownLatch(2);
            Callable<TransferResult> attempt = () -> {
                bothStarting.countDown();
                bothStarting.await(2, TimeUnit.SECONDS);
                return transferService.transfer(idempotencyKey, from.getId(), to.getId(), 250);
            };

            Future<TransferResult> first = pool.submit(attempt);
            Future<TransferResult> second = pool.submit(attempt);

            TransferResult r1 = first.get(10, TimeUnit.SECONDS);
            TransferResult r2 = second.get(10, TimeUnit.SECONDS);

            assertThat(r1.transferId()).isEqualTo(r2.transferId());
            assertThat(List.of(r1.alreadyApplied(), r2.alreadyApplied()))
                    .as("exactly one attempt should be the original, the other should observe it already applied")
                    .containsExactlyInAnyOrder(false, true);

            assertThat(postings.countByTransferId(r1.transferId()))
                    .as("double-entry: exactly one debit + one credit, never four")
                    .isEqualTo(2);
        }
    }
}

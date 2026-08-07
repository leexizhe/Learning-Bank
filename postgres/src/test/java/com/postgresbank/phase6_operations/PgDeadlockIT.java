package com.postgresbank.phase6_operations;

import static com.postgresbank.testsupport.TestSupport.openAccount;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * The database's own deadlock, completing the story {@code WriteSkewIT} starts with SQLSTATE {@code 40001}.
 *
 * <p>The concurrency module deadlocks two Java threads on two {@code ReentrantLock}s and proves it with {@code
 * ThreadMXBean}. This is the same circular wait one layer down, on two database rows — and the interesting difference
 * is what happens next. <b>A JVM deadlock hangs forever; Postgres breaks its own.</b> Every backend that waits longer
 * than {@code deadlock_timeout} (1s by default) runs a wait-graph check, and if it finds a cycle it picks a victim and
 * aborts it with SQLSTATE {@code 40P01}. The survivor commits.
 *
 * <p>That changes what "handle deadlocks" means. In Java you prevent them by lock ordering, because there is no
 * recovery. In Postgres you still prevent them the same way — {@code SELECT ... FOR UPDATE} in ascending id order is
 * the database version of {@code LockOrderedTransferService} — but you must also <em>retry</em>, because the engine
 * will occasionally shoot one of your transactions on purpose. Same retry loop that {@code 40001} needs, which is why
 * both codes belong in the same catch.
 *
 * <p>Which side loses is Postgres's choice, so the test asserts that <b>exactly one</b> transaction failed with {@code
 * 40P01} rather than assuming which. Assuming would make this flaky for no reason.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PgDeadlockIT extends TestContainerConfig {

    /** SQLSTATE 40P01 — deadlock_detected. */
    private static final String DEADLOCK_DETECTED = "40P01";

    private final ExecutorService pool = Executors.newFixedThreadPool(2);

    @Autowired
    private AccountRepository accounts;

    @Autowired
    private DataSource dataSource;

    @Test
    void twoTransactionsLockingTwoRowsInOppositeOrdersDeadlockAndPostgresKillsOne() throws Exception {
        Account first = openAccount(accounts);
        Account second = openAccount(accounts);

        // Both take their first row lock, meet at the barrier, then reach for the other's - so the cycle is guaranteed
        // rather than raced for.
        CountDownLatch bothHoldFirstLock = new CountDownLatch(2);
        AtomicReference<String> failureState = new AtomicReference<>();

        Future<?> forward = pool.submit(lockInOrder(first.getId(), second.getId(), bothHoldFirstLock, failureState));
        Future<?> reverse = pool.submit(lockInOrder(second.getId(), first.getId(), bothHoldFirstLock, failureState));

        forward.get(30, TimeUnit.SECONDS);
        reverse.get(30, TimeUnit.SECONDS);

        assertThat(failureState.get())
                .as("Postgres detected the cycle and aborted exactly one of the two - it does not hang")
                .isEqualTo(DEADLOCK_DETECTED);
    }

    private Runnable lockInOrder(
            long firstId, long secondId, CountDownLatch bothHoldFirstLock, AtomicReference<String> failureState) {
        return () -> {
            try (Connection c = dataSource.getConnection()) {
                c.setAutoCommit(false);
                try {
                    lockAccount(c, firstId);
                    bothHoldFirstLock.countDown();
                    bothHoldFirstLock.await();

                    lockAccount(c, secondId);
                    c.commit();
                } catch (SQLException e) {
                    // Only one side gets here; record its SQLSTATE.
                    failureState.compareAndSet(null, e.getSQLState());
                    c.rollback();
                }
            } catch (SQLException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException(e);
            }
        };
    }

    private void lockAccount(Connection c, long accountId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("select id from accounts where id = ? for update")) {
            ps.setLong(1, accountId);
            ps.executeQuery().close();
        }
    }
}

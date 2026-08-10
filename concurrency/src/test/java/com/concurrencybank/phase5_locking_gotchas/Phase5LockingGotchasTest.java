package com.concurrencybank.phase5_locking_gotchas;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Four separate locking lessons, collected here because the phase is a checklist rather than a single argument: two
 * hazards — locking on the wrong <em>object</em> (an interned string literal), and confusing the class monitor with an
 * instance monitor — then two patterns that get it right, per-key locking and singleton initialisation.
 *
 * <p>Two of the blocks below are measured the same way — run two 100ms operations on two threads and time them. If
 * they overlap the locks were independent; if they take twice as long, something serialised them that shouldn't have.
 * {@link #elapsedRunningBoth} is that measurement, shared so the comparison between the blocks is like-for-like.
 */
class Phase5LockingGotchasTest {

    /** Long enough that thread scheduling noise can't fake either outcome, short enough to keep the suite quick. */
    private static final long WORK_MILLIS = 100;

    /**
     * Runs both actions on their own threads and returns the wall-clock time until both finish. Deliberately <b>not</b>
     * {@code runConcurrently} — that name belongs to the statically imported
     * {@link com.concurrencybank.testutil.ConcurrencyHarness} harness used by the other two blocks, and a member of the
     * same name here would shadow that import across this entire class, nested blocks included.
     */
    private static Duration elapsedRunningBoth(Runnable first, Runnable second) throws InterruptedException {
        Instant start = Instant.now();
        Thread t1 = new Thread(first);
        Thread t2 = new Thread(second);
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        return Duration.between(start, Instant.now());
    }

    /** Locking on an interned string literal, and the striped-lock fix. Both measured with the same stopwatch. */
    @Nested
    class StringLockBugTests {

        private void runQuietly(InterruptibleAction action) {
            try {
                action.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Test
        void unrelatedOperationsSerializeBecauseTheySharedAnInternedStringLock() throws InterruptedException {
            StringLockBugDemo demo = new StringLockBugDemo();

            Duration elapsed = elapsedRunningBoth(
                    () -> runQuietly(() -> demo.writeAuditLog("entry", WORK_MILLIS)),
                    () -> runQuietly(() -> demo.postLedgerEntry(500, WORK_MILLIS)));

            // Two logically-unrelated 100ms operations should overlap and take ~100ms. Because both synchronize on the
            // same interned literal, they instead serialize and take ~200ms.
            assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(WORK_MILLIS * 2));
        }

        @Test
        void lockStripedRegistryLetsUnrelatedKeysRunConcurrently() throws InterruptedException {
            LockStripedRegistry registry = new LockStripedRegistry();

            Duration elapsed = elapsedRunningBoth(
                    () -> runQuietly(() -> registry.runExclusively("audit-log", () -> Thread.sleep(WORK_MILLIS))),
                    () -> runQuietly(() -> registry.runExclusively("ledger", () -> Thread.sleep(WORK_MILLIS))));

            // Different keys -> different lock objects -> genuinely concurrent.
            assertThat(elapsed).isLessThan(Duration.ofMillis(WORK_MILLIS * 2));
        }
    }

    /** {@code static synchronized} locks the {@code Class}; plain {@code synchronized} locks the instance. */
    @Nested
    class BankRegistryTests {

        @Test
        void twoInstancesInstanceMethodsDontBlockEachOther() throws InterruptedException {
            BankRegistry branchOne = new BankRegistry();
            BankRegistry branchTwo = new BankRegistry();

            Duration elapsed = elapsedRunningBoth(
                    () -> branchOne.recordActivity(WORK_MILLIS), () -> branchTwo.recordActivity(WORK_MILLIS));

            assertThat(elapsed).isLessThan(Duration.ofMillis(WORK_MILLIS * 2));
        }

        @Test
        void twoThreadsCallingTheStaticMethodDoSerialize() throws InterruptedException {
            Duration elapsed = elapsedRunningBoth(
                    () -> BankRegistry.nextTransactionId(WORK_MILLIS),
                    () -> BankRegistry.nextTransactionId(WORK_MILLIS));

            // Same class -> same lock -> the second caller waits for the first.
            assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(WORK_MILLIS * 2));
        }

        @Test
        void theStaticLockAndAnInstanceLockOnTheSameObjectAreIndependent() throws InterruptedException {
            BankRegistry registry = new BankRegistry();

            Duration elapsed = elapsedRunningBoth(
                    () -> BankRegistry.nextTransactionId(WORK_MILLIS), () -> registry.recordActivity(WORK_MILLIS));

            // Class-level lock and this same object's instance-level lock are two different monitors, so these don't
            // block each other either.
            assertThat(elapsed).isLessThan(Duration.ofMillis(WORK_MILLIS * 2));
        }
    }

    /** Per-key locking done right: the same stress shape as phase 1, but with contention spread across accounts. */
    @Nested
    class ConcurrentLedgerTests {

        @Test
        void concurrentDepositsAreAllConserved() throws InterruptedException {
            ConcurrentLedger ledger = new ConcurrentLedger();
            int threads = 50;
            int depositsPerThread = 10_000;

            runConcurrently(threads, () -> {
                for (int i = 0; i < depositsPerThread; i++) {
                    ledger.deposit("acc-1", 1);
                }
            });

            assertThat(ledger.getBalance("acc-1")).isEqualTo((long) threads * depositsPerThread);
        }

        @Test
        void concurrentWithdrawalsNeverOverdraw() throws InterruptedException {
            int threads = 200;
            ConcurrentLedger ledger = new ConcurrentLedger();
            ledger.deposit("acc-1", threads); // exactly enough for one successful withdrawal each
            AtomicInteger successes = new AtomicInteger();

            runConcurrently(threads, () -> {
                if (ledger.withdraw("acc-1", 1)) {
                    successes.incrementAndGet();
                }
            });

            assertThat(successes.get()).isEqualTo(threads);
            assertThat(ledger.getBalance("acc-1")).isZero();
        }

        @Test
        void differentAccountsDontInterfereWithEachOther() throws InterruptedException {
            ConcurrentLedger ledger = new ConcurrentLedger();
            int threads = 100;

            runConcurrently(threads, () -> {
                String accountId = "acc-" + (Thread.currentThread().hashCode() % 10);
                ledger.deposit(accountId, 5);
            });

            long total = 0;
            for (int i = 0; i < 10; i++) {
                total += ledger.getBalance("acc-" + i);
            }
            assertThat(total).isEqualTo(threads * 5L);
        }
    }

    /** Two correct singleton idioms, asserted identically — the point is that neither ever builds a second instance. */
    @Nested
    class SingletonIdiomTests {

        @Test
        void doubleCheckedLockingSingletonIsCreatedExactlyOnceUnderConcurrentCallers() throws InterruptedException {
            Set<ExchangeRateService> seenInstances = ConcurrentHashMap.newKeySet();

            runConcurrently(200, () -> seenInstances.add(ExchangeRateService.getInstance()));

            assertThat(seenInstances).hasSize(1);
        }

        @Test
        void holderIdiomSingletonIsAlsoCreatedExactlyOnceUnderConcurrentCallers() throws InterruptedException {
            Set<ExchangeRateServiceHolder> seenInstances = ConcurrentHashMap.newKeySet();

            runConcurrently(200, () -> seenInstances.add(ExchangeRateServiceHolder.getInstance()));

            assertThat(seenInstances).hasSize(1);
        }
    }
}

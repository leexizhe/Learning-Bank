package com.concurrencybank.phase1_threadsafety;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The whole of phase 1 in one file, because the lesson is a comparison and a comparison has to be read side by side.
 * The first three nested blocks fire the identical stress shape — {@link #DEPOSIT_THREADS} threads each doing
 * {@link #DEPOSITS_PER_THREAD} deposits, from the same constants — at a broken counter and at two fixed accounts.
 * Only the final assertion differs, and that difference is the entire point of the phase.
 *
 * <p>This is deliberately not the repo's usual one-test-class-per-production-class layout. It works here because every
 * test is short and teaches one idea; a phase whose tests need their own fixtures or setup should stay split.
 */
class Phase1ThreadSafetyTest {

    private static final int DEPOSIT_THREADS = 50;
    private static final int DEPOSITS_PER_THREAD = 10_000;
    private static final long EXPECTED_TOTAL = (long) DEPOSIT_THREADS * DEPOSITS_PER_THREAD;

    /** One thread per unit of balance, so a correct account grants every withdrawal and lands exactly on zero. */
    private static final int WITHDRAW_THREADS = 200;

    /**
     * A lost update is a race, not a guarantee, so the stress runs up to this many times and stops at the first
     * sighting. Measured on 2 and 4 CPUs, the first attempt suffices in 39 of 40 trials and none exhausted the budget.
     */
    private static final int LOST_UPDATE_ATTEMPTS = 5;

    /** The "before" picture: {@code balance += amount} is read-modify-write, so concurrent increments get lost. */
    @Nested
    class UnsafeCounterTests {

        /**
         * The one test in the phase that asserts an anomaly rather than a guarantee, which takes some care to keep
         * honest — the JMM <em>permits</em> the lost update, it does not promise one, and the two ways of not seeing it
         * are both real. The JIT hoisting the increment out of the loop is handled in {@link UnsafeCounter} by making
         * the field {@code volatile}; the other way is having no parallelism to race on, which no amount of retrying
         * fixes, so it is skipped instead. On one CPU the harness's virtual threads never interleave at all: a
         * compute-only loop hits no blocking point, so each thread runs to completion on the single carrier and the
         * total comes out exact. Measured pinned to one core, the stress reported the arithmetic total 20 times out of
         * 20.
         */
        @Test
        void concurrentDepositsLoseUpdates() throws InterruptedException {
            assumeTrue(Runtime.getRuntime().availableProcessors() > 1, "a data race needs two CPUs to race on");

            long observed = EXPECTED_TOTAL;
            for (int attempt = 0; attempt < LOST_UPDATE_ATTEMPTS && observed == EXPECTED_TOTAL; attempt++) {
                UnsafeCounter counter = new UnsafeCounter();

                runConcurrently(DEPOSIT_THREADS, () -> {
                    for (int i = 0; i < DEPOSITS_PER_THREAD; i++) {
                        counter.deposit(1);
                    }
                });

                observed = counter.getBalance();
            }

            assertThat(observed)
                    .as("unsynchronized balance++ should drop updates under contention")
                    .isLessThan(EXPECTED_TOTAL);
        }
    }

    /** Fix one: object-level intrinsic locking. Same stress shape as above, opposite outcome. */
    @Nested
    class SynchronizedAccountTests {

        @Test
        void concurrentDepositsNeverLoseAnUpdate() throws InterruptedException {
            SynchronizedAccount account = new SynchronizedAccount();

            runConcurrently(DEPOSIT_THREADS, () -> {
                for (int i = 0; i < DEPOSITS_PER_THREAD; i++) {
                    account.deposit(1);
                }
            });

            assertThat(account.getBalance()).isEqualTo(EXPECTED_TOTAL);
        }

        @Test
        void concurrentWithdrawalsNeverOverdraw() throws InterruptedException {
            SynchronizedAccount account = new SynchronizedAccount();
            account.deposit(WITHDRAW_THREADS); // exactly enough for one successful $1 withdrawal each
            AtomicInteger successes = new AtomicInteger();

            runConcurrently(WITHDRAW_THREADS, () -> {
                if (account.withdraw(1)) {
                    successes.incrementAndGet();
                }
            });

            assertThat(successes.get()).isEqualTo(WITHDRAW_THREADS);
            assertThat(account.getBalance()).isZero();
        }
    }

    /** Fix two: lock-free, via {@code addAndGet} and a compare-and-swap retry loop. Same shape, same outcome. */
    @Nested
    class AtomicAccountTests {

        @Test
        void concurrentDepositsNeverLoseAnUpdate() throws InterruptedException {
            AtomicAccount account = new AtomicAccount();

            runConcurrently(DEPOSIT_THREADS, () -> {
                for (int i = 0; i < DEPOSITS_PER_THREAD; i++) {
                    account.deposit(1);
                }
            });

            assertThat(account.getBalance()).isEqualTo(EXPECTED_TOTAL);
        }

        @Test
        void concurrentWithdrawalsNeverOverdraw() throws InterruptedException {
            AtomicAccount account = new AtomicAccount();
            account.deposit(WITHDRAW_THREADS);
            AtomicInteger successes = new AtomicInteger();

            runConcurrently(WITHDRAW_THREADS, () -> {
                if (account.withdraw(1)) {
                    successes.incrementAndGet();
                }
            });

            assertThat(successes.get()).isEqualTo(WITHDRAW_THREADS);
            assertThat(account.getBalance()).isZero();
        }
    }

    /**
     * The follow-up question, and a different kind of test: no threads and no harness. ABA is a property of the
     * sequence of values, not of timing, so performing A -> B -> A on one thread demonstrates it exactly and
     * deterministically. A stress loop would prove the same thing less clearly.
     */
    @Nested
    class AbaProblemDemoTests {

        @Test
        void plainCasCannotTellThatTheValueLeftAndCameBack() {
            AbaProblemDemo slot = new AbaProblemDemo("A");

            String observed = slot.read();
            slot.set("B");
            slot.set("A");

            assertThat(slot.compareAndSet(observed, "C"))
                    .as("the CAS succeeded even though the world changed twice underneath it")
                    .isTrue();
        }

        @Test
        void aStampMakesTheInterveningWritesVisible() {
            AbaProblemDemo slot = new AbaProblemDemo("A");

            int[] stamp = new int[1];
            String observed = slot.readStamped(stamp);
            slot.set("B");
            slot.set("A");

            assertThat(slot.compareAndSetStamped(observed, stamp[0], "C"))
                    .as("same reference, different stamp - the CAS correctly refuses")
                    .isFalse();
        }

        @Test
        void anUncontendedStampedCasStillSucceeds() {
            AbaProblemDemo slot = new AbaProblemDemo("A");

            int[] stamp = new int[1];
            String observed = slot.readStamped(stamp);

            assertThat(slot.compareAndSetStamped(observed, stamp[0], "C"))
                    .as("nothing happened in between, so the stamp is no obstacle")
                    .isTrue();
        }
    }
}

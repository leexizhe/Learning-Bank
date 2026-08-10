package com.concurrencybank.phase2_deadlock;

import static com.concurrencybank.testutil.DeadlockProbe.awaitDeadlockAmong;
import static com.concurrencybank.testutil.DeadlockProbe.deadlockedThreadIds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The same trap and its fix, asserted from both directions with the same witness. Two accounts, opposite-direction
 * transfers, and {@code ThreadMXBean}'s lock-graph analysis rather than a stopwatch: the naive service is asserted to
 * genuinely deadlock, the lock-ordered one to never form a cycle while its threads are actually contending.
 *
 * <p>Asserting only that the fixed version finishes quickly would be weak evidence — a slow machine looks like a
 * deadlock and a lucky interleaving looks like a fix. Asserting both directions with the JVM's own detector is what
 * makes the pair conclusive.
 *
 * <p><b>Both probe calls are scoped to their own worker threads on purpose.</b> {@code findDeadlockedThreads()} is
 * JVM-global, Surefire shares one fork across every test class, and the naive demo below leaves two threads deadlocked
 * forever by design. An unscoped assertion in either block would pass or fail depending on execution order.
 */
class Phase2DeadlockTest {

    private static final long STARTING_BALANCE = 100_000;

    /**
     * Proves the trap is real, not merely that the fix is fast.
     *
     * <p><b>The two threads started here never finish.</b> That is the point, and it dictates the rest of the design:
     * they are <b>daemon</b> threads and are never {@code join()}ed, so the Surefire fork can still exit at the end of
     * the run. A non-daemon deadlocked thread would hang the build permanently — which is exactly the production
     * failure being demonstrated, and a poor property for a test to have.
     */
    @Nested
    class NaiveTransferServiceTests {

        @Test
        void lockingInCallerOrderDeadlocksAndTheJvmItselfConfirmsIt() throws InterruptedException {
            LockedAccount accountOne = new LockedAccount(1, STARTING_BALANCE);
            LockedAccount accountTwo = new LockedAccount(2, STARTING_BALANCE);
            NaiveTransferService service = new NaiveTransferService();

            // Neither thread asks for its second lock until both are holding their
            // first. Without this the test would be hoping for an interleaving;
            // with it, the circular wait is guaranteed on every run.
            CyclicBarrier bothHoldFirstLock = new CyclicBarrier(2);

            Thread oneToTwo = deadlockingThread(
                    "naive-transfer-1-to-2",
                    () -> service.transfer(accountOne, accountTwo, 1, () -> await(bothHoldFirstLock)));
            Thread twoToOne = deadlockingThread(
                    "naive-transfer-2-to-1",
                    () -> service.transfer(accountTwo, accountOne, 1, () -> await(bothHoldFirstLock)));

            oneToTwo.start();
            twoToOne.start();

            Set<Long> expected = Set.of(oneToTwo.threadId(), twoToOne.threadId());
            Set<Long> deadlocked = awaitDeadlockAmong(expected, Duration.ofSeconds(10));

            assertThat(deadlocked)
                    .as("the JVM's own deadlock detector names both transfer threads as a cycle")
                    .containsAll(expected);
            assertThat(accountOne.getBalance() + accountTwo.getBalance())
                    .as("neither transfer completed, so no money moved")
                    .isEqualTo(STARTING_BALANCE * 2);
        }

        /**
         * Daemon, and deliberately never joined — see this block's javadoc. Named so that a thread dump taken during
         * the run reads clearly.
         */
        private Thread deadlockingThread(String name, Runnable body) {
            return Thread.ofPlatform().name(name).daemon(true).unstarted(body);
        }

        private void await(CyclicBarrier barrier) {
            try {
                barrier.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (BrokenBarrierException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /** The fix, and the mirror assertion: same two accounts, same opposite-direction load, no cycle ever forms. */
    @Nested
    class LockOrderedTransferServiceTests {

        /**
         * The classic trap: one thread repeatedly transfers A -> B while another repeatedly transfers B -> A, at the
         * same time. Without consistent lock ordering this reliably deadlocks (each thread grabs its "from" account and
         * blocks forever waiting for the other's). With ordering-by-id, both threads always fight over account 1's lock
         * first, so there's real contention but never a circular wait.
         *
         * <p>Two independent witnesses, because a timeout alone can't distinguish "no deadlock" from "deadlock that
         * resolved" or from "machine was slow". {@code assertTimeoutPreemptively} bounds the run, and {@code
         * ThreadMXBean} is polled <b>while the two threads are actually contending</b>.
         */
        @Test
        void concurrentOppositeDirectionTransfersNeverDeadlockAndConserveTheTotal() {
            LockedAccount accountOne = new LockedAccount(1, STARTING_BALANCE);
            LockedAccount accountTwo = new LockedAccount(2, STARTING_BALANCE);
            long totalBefore = accountOne.getBalance() + accountTwo.getBalance();
            LockOrderedTransferService service = new LockOrderedTransferService();
            int roundsPerDirection = 5_000;

            Set<Long> workerIds = ConcurrentHashMap.newKeySet();
            Runnable oneToTwo = () -> {
                workerIds.add(Thread.currentThread().threadId());
                for (int i = 0; i < roundsPerDirection; i++) {
                    service.transfer(accountOne, accountTwo, 1);
                }
            };
            Runnable twoToOne = () -> {
                workerIds.add(Thread.currentThread().threadId());
                for (int i = 0; i < roundsPerDirection; i++) {
                    service.transfer(accountTwo, accountOne, 1);
                }
            };

            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                ExecutorService pool = Executors.newFixedThreadPool(2);
                try {
                    Future<?> first = pool.submit(oneToTwo);
                    Future<?> second = pool.submit(twoToOne);

                    while (!first.isDone() || !second.isDone()) {
                        Set<Long> stuckWorkers = new HashSet<>(deadlockedThreadIds());
                        stuckWorkers.retainAll(workerIds);
                        assertThat(stuckWorkers)
                                .as("the JVM's deadlock detector, watching mid-flight, sees no cycle here")
                                .isEmpty();
                        Thread.sleep(20);
                    }

                    first.get();
                    second.get();
                } finally {
                    pool.shutdown();
                }
            });

            assertThat(accountOne.getBalance() + accountTwo.getBalance()).isEqualTo(totalBefore);
        }
    }
}

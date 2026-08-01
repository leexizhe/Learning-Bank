package com.concurrencybank.phase2_deadlock;

import static com.concurrencybank.testutil.DeadlockProbe.deadlockedThreadIds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

class LockOrderedTransferServiceTest {

    /**
     * The classic trap: one thread repeatedly transfers A -> B while another
     * repeatedly transfers B -> A, at the same time. Without consistent lock
     * ordering this reliably deadlocks (each thread grabs its "from" account
     * and blocks forever waiting for the other's). With ordering-by-id, both
     * threads always fight over account 1's lock first, so there's real
     * contention but never a circular wait.
     *
     * <p>Two independent witnesses, because a timeout alone is weak evidence -
     * it can't distinguish "no deadlock" from "deadlock that resolved" or from
     * "machine was slow". {@code assertTimeoutPreemptively} bounds the run, and
     * {@code ThreadMXBean} is polled <b>while the two threads are actually
     * contending</b> and asserts the JVM never sees a cycle between them. That
     * is the exact mirror of what {@code NaiveTransferServiceDeadlockTest}
     * asserts about the unordered version, using the same probe.
     *
     * <p>The probe is scoped to this test's own worker threads on purpose:
     * {@code findDeadlockedThreads()} is JVM-global, Surefire shares one fork
     * across every test class, and the naive demo leaves two threads deadlocked
     * forever by design. An unscoped {@code isEmpty()} assertion here would pass
     * or fail depending on class ordering.
     */
    @Test
    void concurrentOppositeDirectionTransfersNeverDeadlockAndConserveTheTotal() {
        LockedAccount accountOne = new LockedAccount(1, 100_000);
        LockedAccount accountTwo = new LockedAccount(2, 100_000);
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

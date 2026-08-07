package com.concurrencybank.phase2_deadlock;

import static com.concurrencybank.testutil.DeadlockProbe.awaitDeadlockAmong;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import org.junit.jupiter.api.Test;

/**
 * Proves the trap is real, not merely that the fix is fast.
 *
 * <p>Most deadlock "tests" assert that the <em>fixed</em> version finishes quickly, which is weak evidence — a slow
 * machine looks like a deadlock and a lucky interleaving looks like a fix. This one asserts the opposite direction: the
 * broken implementation genuinely deadlocks, and the witness is {@code ThreadMXBean.findDeadlockedThreads()} — the
 * JVM's own lock-graph analysis, not a timeout.
 *
 * <p><b>The two threads started here never finish.</b> That is the point, and it dictates the rest of the design: they
 * are <b>daemon</b> threads and are never {@code join()}ed, so the Surefire fork can still exit at the end of the run.
 * A non-daemon deadlocked thread would hang the build permanently — which is exactly the production failure being
 * demonstrated, and a poor property for a test to have.
 */
class NaiveTransferServiceDeadlockTest {

    @Test
    void lockingInCallerOrderDeadlocksAndTheJvmItselfConfirmsIt() throws InterruptedException {
        LockedAccount accountOne = new LockedAccount(1, 100_000);
        LockedAccount accountTwo = new LockedAccount(2, 100_000);
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
                .isEqualTo(200_000);
    }

    /**
     * Daemon, and deliberately never joined — see the class javadoc. Named so that a thread dump taken during the run
     * reads clearly.
     */
    private static Thread deadlockingThread(String name, Runnable body) {
        return Thread.ofPlatform().name(name).daemon(true).unstarted(body);
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (BrokenBarrierException e) {
            throw new IllegalStateException(e);
        }
    }
}

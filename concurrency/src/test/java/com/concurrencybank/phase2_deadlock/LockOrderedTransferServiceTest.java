package com.concurrencybank.phase2_deadlock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
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
     * contention but never a circular wait. assertTimeoutPreemptively fails the
     * test loudly instead of hanging the build if that guarantee ever breaks.
     */
    @Test
    void concurrentOppositeDirectionTransfersNeverDeadlockAndConserveTheTotal() {
        LockedAccount accountOne = new LockedAccount(1, 100_000);
        LockedAccount accountTwo = new LockedAccount(2, 100_000);
        long totalBefore = accountOne.getBalance() + accountTwo.getBalance();
        LockOrderedTransferService service = new LockOrderedTransferService();
        int roundsPerDirection = 5_000;

        Runnable oneToTwo = () -> {
            for (int i = 0; i < roundsPerDirection; i++) {
                service.transfer(accountOne, accountTwo, 1);
            }
        };
        Runnable twoToOne = () -> {
            for (int i = 0; i < roundsPerDirection; i++) {
                service.transfer(accountTwo, accountOne, 1);
            }
        };

        assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                Future<?> first = pool.submit(oneToTwo);
                Future<?> second = pool.submit(twoToOne);
                first.get();
                second.get();
            } finally {
                pool.shutdown();
            }
        });

        assertThat(accountOne.getBalance() + accountTwo.getBalance()).isEqualTo(totalBefore);
    }
}

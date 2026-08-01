package com.concurrencybank.phase8_memorymodel;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class FinalFieldFreezeTest {

    private static final int ITERATIONS = 200_000;

    /**
     * The same race as {@code UnsafePublicationTest}, against the same plain
     * publishing field, differing only in that the holder's field is
     * {@code final}. Here the anomaly is not merely unlikely but forbidden — the
     * freeze at the end of the constructor emits a store-store barrier, so the
     * field write can never be reordered past the reference publication — which
     * is why this one gets a real assertion rather than a printout.
     */
    @Test
    void aFinalFieldIsNeverSeenAsItsDefaultValue() throws InterruptedException {
        FinalFieldFreeze publication = new FinalFieldFreeze();
        AtomicLong reads = new AtomicLong();
        AtomicLong sawDefault = new AtomicLong();
        AtomicBoolean done = new AtomicBoolean();

        Thread reader = Thread.ofPlatform().name("final-field-reader").start(() -> {
            while (!done.get()) {
                FinalFieldFreeze.Holder holder = publication.read();
                if (holder != null) {
                    reads.incrementAndGet();
                    if (holder.getValue() == 0) {
                        sawDefault.incrementAndGet();
                    }
                }
            }
        });

        for (int i = 0; i < ITERATIONS; i++) {
            publication.reset();
            publication.publish(42);
        }
        done.set(true);
        reader.join(Duration.ofSeconds(10));

        assertThat(reads.get()).as("the race actually ran").isPositive();
        assertThat(sawDefault.get())
                .as("the final-field freeze forbids observing a partially constructed Holder")
                .isZero();
    }
}

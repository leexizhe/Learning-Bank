package com.concurrencybank.phase8_memorymodel;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The tests in this phase are asymmetric on purpose, and the asymmetry is the lesson: <b>guarantees get assertions,
 * anomalies get printouts</b>. That rule is applied twice below, which is why the file is worth reading in one pass.
 *
 * <p>{@link FinalFieldFreezeTests} and {@link VolatileFlagTests} <b>assert</b> their outcome, because the JMM
 * guarantees it and a guarantee is a thing you can assert. {@link UnsafePublicationTests} and
 * {@link PlainFlagTests} do <b>not</b> assert that the broken versions break, because the JMM only <em>permits</em>
 * that. On x86, whose TSO model forbids store-store reordering in hardware, HotSpot will usually decline to
 * demonstrate it; asserting the anomaly occurs would produce a test that fails on the machines where the code is
 * safest, which is exactly backwards.
 *
 * <p>So for the anomalies: run the race, report what was observed, assert only that the harness completed. If a count
 * is ever non-zero on your machine, that is a genuine sighting of a JMM violation and worth remembering. If it's
 * always zero — which it will be on most desktops — the correct conclusion is "my hardware hides this", not "the code
 * is fine".
 */
class Phase8MemoryModelTest {

    /** Both publication races run the same loop the same number of times; only the holder's field differs. */
    private static final int ITERATIONS = 200_000;

    /** The anomaly: a plain field can be published before the write that initialises it becomes visible. */
    @Nested
    class UnsafePublicationTests {

        @Test
        void aNonFinalFieldMayBePublishedBeforeItIsInitialised() throws InterruptedException {
            UnsafePublication publication = new UnsafePublication();
            AtomicLong reads = new AtomicLong();
            AtomicLong sawDefault = new AtomicLong();
            AtomicBoolean done = new AtomicBoolean();

            Thread reader = Thread.ofPlatform()
                    .name("unsafe-publication-reader")
                    .start(() -> {
                        while (!done.get()) {
                            UnsafePublication.Holder holder = publication.read();
                            if (holder != null) {
                                reads.incrementAndGet();
                                if (holder.getValue() == 0) {
                                    // A value nobody ever wrote: the reference became visible before the field write
                                    // behind it.
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

            System.out.printf(
                    "UnsafePublication: %,d reads observed, %,d saw the uninitialised default%n",
                    reads.get(), sawDefault.get());

            assertThat(reads.get())
                    .as("the race actually ran - if this is zero the experiment proved nothing either way")
                    .isPositive();
        }
    }

    /** The guarantee: the same race, one keyword different, and now the anomaly is forbidden rather than unlikely. */
    @Nested
    class FinalFieldFreezeTests {

        /**
         * The same race as {@link UnsafePublicationTests}, against the same plain publishing field, differing only in
         * that the holder's field is {@code final}. The freeze at the end of the constructor emits a store-store
         * barrier, so the field write can never be reordered past the reference publication — which is why this one
         * gets a real assertion rather than a printout.
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

    /** The guarantee, so it gets a real assertion. */
    @Nested
    class VolatileFlagTests {

        @Test
        void aVolatileFlagIsAlwaysNoticedByASpinningReader() throws InterruptedException {
            StopFlagVisibility flags = new StopFlagVisibility();
            CountDownLatch spinning = new CountDownLatch(1);
            AtomicBoolean finished = new AtomicBoolean();

            Thread reader = Thread.ofPlatform().name("volatile-flag-reader").start(() -> {
                spinning.countDown();
                flags.spinUntilVolatileFlagClears();
                finished.set(true);
            });

            spinning.await();
            flags.stopVolatile();
            reader.join(Duration.ofSeconds(10));

            assertThat(finished.get())
                    .as("volatile creates the happens-before edge that makes the write visible")
                    .isTrue();
        }
    }

    /**
     * The mirror image of {@link VolatileFlagTests}, and the same treatment {@link UnsafePublicationTests} gets:
     * whether the JIT hoists the plain read out of the loop depends on how long the loop ran before C2 compiled it,
     * which is not a property a build should depend on in either direction. Asserting it hangs would fail whenever the
     * JIT declines; asserting it terminates would fail whenever it doesn't. So the outcome is reported, not asserted.
     */
    @Nested
    class PlainFlagTests {

        /**
         * The reader is a <b>daemon</b> thread and is never joined without a timeout — if the hoist does occur, that
         * thread spins until the JVM exits, and a non-daemon thread would hang the build forever.
         */
        @Test
        void aPlainFlagMayNeverBeNoticed() throws InterruptedException {
            StopFlagVisibility flags = new StopFlagVisibility();
            CountDownLatch spinning = new CountDownLatch(1);
            CountDownLatch stopped = new CountDownLatch(1);

            Thread reader = Thread.ofPlatform()
                    .name("plain-flag-reader")
                    .daemon(true)
                    .start(() -> {
                        spinning.countDown();
                        flags.spinUntilPlainFlagClears();
                        stopped.countDown();
                    });

            spinning.await();
            // Let the loop run long enough for C2 to consider compiling it.
            Thread.sleep(200);
            flags.stopPlain();

            boolean noticed = stopped.await(2, TimeUnit.SECONDS);
            System.out.println("StopFlagVisibility: the plain-flag reader "
                    + (noticed
                            ? "did notice the write (the JIT did not hoist the read this time)"
                            : "never noticed the write - the read was hoisted out of the loop"));

            assertThat(reader.isAlive() || noticed)
                    .as("the experiment ran; which way it went is reported, not asserted")
                    .isTrue();
        }
    }
}

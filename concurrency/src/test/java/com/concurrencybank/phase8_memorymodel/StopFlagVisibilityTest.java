package com.concurrencybank.phase8_memorymodel;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class StopFlagVisibilityTest {

    /** The guarantee, so it gets a real assertion. */
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

    /**
     * The anomaly, so it gets a printout rather than an assertion — see {@code UnsafePublicationTest} for the full
     * reasoning.
     *
     * <p>Whether the JIT hoists the plain read out of the loop depends on how long the loop ran before C2 compiled it,
     * which is not a property a build should depend on in either direction. Asserting it hangs would fail whenever the
     * JIT declines; asserting it terminates would fail whenever it doesn't. So this reports which happened.
     *
     * <p>The reader is a <b>daemon</b> thread and is never joined without a timeout — if the hoist does occur, that
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

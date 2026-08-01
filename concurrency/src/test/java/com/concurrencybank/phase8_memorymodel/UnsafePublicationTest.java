package com.concurrencybank.phase8_memorymodel;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The pair of tests in this package are asymmetric on purpose, and the asymmetry is the lesson.
 *
 * <p>{@link FinalFieldFreezeTest} <b>asserts</b> that the final-field version never shows a
 * partially-constructed object, because the JMM guarantees it and a guarantee is a thing you can
 * assert. This test does <b>not</b> assert that the non-final version breaks, because the JMM only
 * <em>permits</em> that — and on x86, whose TSO model forbids store-store reordering in hardware,
 * HotSpot will usually decline to demonstrate it. Asserting the anomaly occurs would produce a test
 * that fails on the machines where the code is safest, which is exactly backwards.
 *
 * <p>So: run the race, report what was observed, assert only that the harness completed. If the
 * count is ever non-zero on your machine, that is a genuine sighting of a JMM violation and worth
 * remembering. If it's always zero — which it will be on most desktops — the correct conclusion is
 * "my hardware hides this", not "the code is fine".
 */
class UnsafePublicationTest {

  private static final int ITERATIONS = 200_000;

  @Test
  void aNonFinalFieldMayBePublishedBeforeItIsInitialised() throws InterruptedException {
    UnsafePublication publication = new UnsafePublication();
    AtomicLong reads = new AtomicLong();
    AtomicLong sawDefault = new AtomicLong();
    AtomicBoolean done = new AtomicBoolean();

    Thread reader =
        Thread.ofPlatform()
            .name("unsafe-publication-reader")
            .start(
                () -> {
                  while (!done.get()) {
                    UnsafePublication.Holder holder = publication.read();
                    if (holder != null) {
                      reads.incrementAndGet();
                      if (holder.getValue() == 0) {
                        // A value nobody ever wrote: the reference became visible
                        // before the field write behind it.
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

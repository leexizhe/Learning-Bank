package com.concurrencybank.phase1_threadsafety;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class AbaProblemDemoTest {

    /**
     * No threads: ABA is a property of the sequence of values, not of timing, so
     * performing A -> B -> A on one thread demonstrates it exactly and
     * deterministically. A stress loop would prove the same thing less clearly.
     */
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

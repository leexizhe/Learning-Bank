package com.acrabank.testsupport;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

/**
 * A real {@link Clock} whose hands you move by hand.
 *
 * <p>This exists so the expiry tests can assert the actual arithmetic - that a token advertised as good for 1799
 * seconds is still used at 1738 and abandoned at 1739 - without a test suite that takes half an hour to run. Sleeping
 * would prove nothing anyway: a passing timing assertion only says the machine was not busy at that moment.
 *
 * <p>Not a mock. It is an implementation of {@code Clock} that returns what it was told to return, and the production
 * code cannot distinguish it from {@code Clock.systemUTC()}.
 */
public final class MutableClock extends Clock {

    private final ZoneId zone;
    private volatile Instant now;

    public MutableClock(Instant start) {
        this(start, ZoneId.of("UTC"));
    }

    private MutableClock(Instant start, ZoneId zone) {
        this.now = start;
        this.zone = zone;
    }

    public void advance(Duration amount) {
        now = now.plus(amount);
    }

    public void reset(Instant to) {
        now = to;
    }

    @Override
    public Instant instant() {
        return now;
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId other) {
        return new MutableClock(now, other);
    }
}

package com.concurrencybank.phase7_primitives;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * "At most {@code limit} requests in any {@code window}" — enforced literally, by remembering when each grant happened
 * and evicting the ones that have aged out. The window slides continuously with the clock rather than resetting on a
 * boundary.
 *
 * <p><b>Why isn't a fixed window enough?</b> This is the follow-up question, and it has a specific answer. A fixed
 * window ("100 per minute, counter resets on the minute") permits <b>twice the limit</b> across a boundary: 100
 * requests at 11:59:59 and 100 more at 12:00:01 is 200 requests in two seconds, and every one of them is inside the
 * stated policy. The downstream you were protecting sees a 2× spike at exactly the moment your dashboard says you were
 * compliant. A sliding window can't produce that, because at 12:00:01 the 11:59:59 grants are still inside the trailing
 * window and still counted — {@code SlidingWindowRateLimiterTest} asserts precisely this difference.
 *
 * <p><b>What it costs:</b> O(limit) memory per limiter, because every grant in the current window is a retained
 * timestamp. That's the honest trade against {@link TokenBucketRateLimiter}, which is O(1) state regardless of rate. At
 * "100/minute per API key" the deque is trivial; at "100,000/second" it is not, and you'd reach for the token bucket or
 * a bucketed approximation (counters per sub-interval, interpolated) instead. Knowing <em>which</em> limiter to reach
 * for, and why, is the actual question being asked.
 *
 * <p>The other behavioural difference: a token bucket lets a rested caller burst the whole bucket instantly, whereas
 * this one admits requests only as old ones expire. Neither is "correct" — bursty is friendlier to clients, strict is
 * kinder to the thing downstream.
 *
 * <p>Clock injected for the same reason as the token bucket: the test advances time by hand rather than sleeping.
 */
public class SlidingWindowRateLimiter {

    private final int limit;
    private final long windowNanos;
    private final LongSupplier nanoTime;

    /**
     * Grant timestamps, oldest first. A deque rather than a queue because eviction happens at the head and insertion at
     * the tail, and both are O(1).
     */
    private final Deque<Long> grants = new ArrayDeque<>();

    public SlidingWindowRateLimiter(int limit, long windowMillis) {
        this(limit, windowMillis, System::nanoTime);
    }

    /** Package-private: the clock seam exists for the test, not for callers. */
    SlidingWindowRateLimiter(int limit, long windowMillis, LongSupplier nanoTime) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive, was " + limit);
        }
        if (windowMillis <= 0) {
            throw new IllegalArgumentException("windowMillis must be positive, was " + windowMillis);
        }
        this.limit = limit;
        this.windowNanos = TimeUnit.MILLISECONDS.toNanos(windowMillis);
        this.nanoTime = nanoTime;
    }

    public synchronized boolean tryAcquire() {
        long now = nanoTime.getAsLong();
        evictExpired(now);
        if (grants.size() >= limit) {
            return false;
        }
        grants.addLast(now);
        return true;
    }

    /** Exposed for the test; the natural gauge to export in production. */
    synchronized int usedInWindow() {
        evictExpired(nanoTime.getAsLong());
        return grants.size();
    }

    /**
     * Drops every grant that has fallen out of the trailing window. The comparison is strict ({@code <=}) so a grant
     * exactly one full window old has expired — otherwise a caller at a perfectly steady rate would find itself one
     * slot short forever.
     */
    private void evictExpired(long now) {
        long cutoff = now - windowNanos;
        while (!grants.isEmpty() && grants.peekFirst() <= cutoff) {
            grants.pollFirst();
        }
    }
}

package com.concurrencybank.phase7_primitives;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * A token bucket: {@code capacity} tokens to spend, refilled at a steady rate. Take a token and you may proceed; find
 * the bucket empty and you're throttled. The shape it gives you is <b>burst then throttle</b> — a caller that has been
 * quiet can spend the whole bucket at once, then drops to the refill rate. That's usually what you want from an API
 * limiter, because it forgives a client that batches its work without letting it exceed the long-run rate.
 *
 * <p><b>There is no timer thread.</b> The naive implementation schedules something to add a token every N milliseconds,
 * which costs a thread (or a shared scheduler slot) per limiter — fine for one, ruinous when you have a limiter per API
 * key and a million keys. Instead {@link #refill()} works out how many tokens <em>would</em> have accrued since the
 * last look, from the elapsed nanos. The bucket is only ever computed when someone asks, so an idle limiter costs
 * exactly nothing.
 *
 * <p><b>The sharp edge worth knowing cold:</b> {@code refill()} advances {@code lastRefillNanos} by <em>exactly the
 * time it converted into tokens</em>, not to {@code now}. Snapping the marker forward to {@code now} silently discards
 * the sub-token remainder on every single call, and since callers poll far more often than one token-interval, that
 * leak compounds until the limiter delivers noticeably less than its configured rate. Integer division plus a preserved
 * remainder is the fix — and it's why this class holds {@code nanosPerToken} rather than a {@code double} rate.
 * Floating-point accumulation would drift for the same reason, one rounding error at a time.
 *
 * <p>The clock is injected rather than hardcoded to {@link System#nanoTime()}, so the test can advance time by hand and
 * assert refill behaviour deterministically instead of sleeping and hoping. Same reasoning as the postgres module's
 * {@code afterRead} / {@code duringHold} seams: a test that sleeps is a test that flakes on a loaded CI box.
 *
 * <p>Everything is {@code synchronized} because a refill is a read-modify-write over two fields at once ({@code tokens}
 * and {@code lastRefillNanos}) — an invariant spanning more than one field is exactly the case a single CAS can't
 * express, so a lock is the honest answer here rather than a cleverer one.
 */
public class TokenBucketRateLimiter {

    private final long capacity;
    private final long nanosPerToken;
    private final LongSupplier nanoTime;

    private long tokens;
    private long lastRefillNanos;

    public TokenBucketRateLimiter(long capacity, long tokensPerSecond) {
        this(capacity, tokensPerSecond, System::nanoTime);
    }

    /** Package-private: the clock seam exists for the test, not for callers. */
    TokenBucketRateLimiter(long capacity, long tokensPerSecond, LongSupplier nanoTime) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive, was " + capacity);
        }
        if (tokensPerSecond <= 0 || tokensPerSecond > TimeUnit.SECONDS.toNanos(1)) {
            throw new IllegalArgumentException("tokensPerSecond out of range: " + tokensPerSecond);
        }
        this.capacity = capacity;
        this.nanosPerToken = TimeUnit.SECONDS.toNanos(1) / tokensPerSecond;
        this.nanoTime = nanoTime;
        // A fresh limiter starts full, so the first caller gets the full burst rather than being throttled for having
        // arrived early.
        this.tokens = capacity;
        this.lastRefillNanos = nanoTime.getAsLong();
    }

    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    public synchronized boolean tryAcquire(long permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be positive, was " + permits);
        }
        refill();
        if (tokens < permits) {
            return false;
        }
        tokens -= permits;
        return true;
    }

    /** Exposed for the test; also the thing you'd export as a gauge in production. */
    synchronized long availableTokens() {
        refill();
        return tokens;
    }

    private void refill() {
        long elapsed = nanoTime.getAsLong() - lastRefillNanos;
        if (elapsed < nanosPerToken) {
            // Not one whole token's worth of time yet. Crucially, leave lastRefillNanos alone — see the class javadoc.
            return;
        }
        long earned = elapsed / nanosPerToken;
        tokens = Math.min(capacity, tokens + earned);
        lastRefillNanos += earned * nanosPerToken;
    }
}

package com.concurrencybank.phase6_async_patterns;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Joiner;

/**
 * {@code ScopedValue} vs {@code ThreadLocal}: a {@code ThreadLocal} is
 * mutable, has no defined end to its lifetime (you have to remember to
 * {@code remove()} it, or it leaks — especially dangerous on a pooled thread
 * that outlives the request it was set for), and does <b>not</b> propagate to
 * a new thread unless you reach for {@code InheritableThreadLocal}, which
 * only copies the value once at thread-creation time.
 *
 * <p>A {@code ScopedValue} is immutable for the lifetime of the binding,
 * bound only for the dynamic extent of one {@code where(...).run/call(...)}
 * block, and automatically unbound the moment that block exits — no
 * {@code remove()} to forget. Crucially for structured concurrency: bindings
 * are automatically visible to subtasks forked from inside that block via
 * {@code StructuredTaskScope}, with no manual propagation code at all.
 */
public final class AuditContext {

    private static final ScopedValue<String> CORRELATION_ID = ScopedValue.newInstance();

    private AuditContext() {}

    public static Optional<String> currentCorrelationId() {
        return CORRELATION_ID.isBound() ? Optional.of(CORRELATION_ID.get()) : Optional.empty();
    }

    /**
     * Runs three "account checks" concurrently under {@code StructuredTaskScope},
     * each reading the ambient correlation id — proving it propagates into
     * forked subtasks automatically, with nothing passed as a parameter.
     */
    public static List<String> runAuditedAccountChecks(String correlationId) throws InterruptedException {
        return ScopedValue.where(CORRELATION_ID, correlationId).call(() -> {
            try (var scope = StructuredTaskScope.open(Joiner.<String>allSuccessfulOrThrow())) {
                var fraud = scope.fork(() -> "fraud-check saw: " + currentCorrelationId().orElseThrow());
                var credit = scope.fork(() -> "credit-check saw: " + currentCorrelationId().orElseThrow());
                var sanctions = scope.fork(() -> "sanctions-check saw: " + currentCorrelationId().orElseThrow());
                scope.join();
                return List.of(fraud.get(), credit.get(), sanctions.get());
            }
        });
    }
}

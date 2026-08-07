package com.concurrencybank.phase5_locking_gotchas;

/**
 * Deliberately broken. String literals are interned by the JVM, so every occurrence of {@code "shared-resource-lock"}
 * in this class file — no matter which method, no matter that the two methods guard completely unrelated state — refers
 * to the exact same {@code String} object at runtime. {@code synchronized} on it means these two logically-independent
 * operations silently serialize against each other, killing throughput for a reason that won't show up anywhere in the
 * code's visible structure.
 *
 * <p>See {@link LockStripedRegistry} for the fix: a private, never-interned lock object per key instead of a shared
 * literal.
 */
public class StringLockBugDemo {

    private final StringBuilder auditLog = new StringBuilder();
    private long ledgerTotal;

    public void writeAuditLog(String entry, long simulatedWorkMillis) throws InterruptedException {
        synchronized ("shared-resource-lock") {
            Thread.sleep(simulatedWorkMillis);
            auditLog.append(entry).append('\n');
        }
    }

    public void postLedgerEntry(long amountMinor, long simulatedWorkMillis) throws InterruptedException {
        synchronized ("shared-resource-lock") {
            Thread.sleep(simulatedWorkMillis);
            ledgerTotal += amountMinor;
        }
    }

    public long getLedgerTotal() {
        return ledgerTotal;
    }
}

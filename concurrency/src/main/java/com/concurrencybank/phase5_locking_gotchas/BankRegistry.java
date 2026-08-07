package com.concurrencybank.phase5_locking_gotchas;

/**
 * {@code static synchronized} locks the {@code Class} object — one lock shared by every instance and every caller,
 * forever. Instance {@code synchronized} locks {@code this} — one lock per instance. The two are entirely different
 * monitors: a thread inside the static method never blocks a thread inside an instance method on the same object, and
 * vice versa. Two different instances' instance methods never block each other either, since each has its own monitor.
 *
 * <p>Bank-flavored: transaction IDs have to be globally unique across the whole bank, so that counter is class-level.
 * Recording a branch's local activity log only needs to be consistent within that one branch, so it's instance-level.
 */
public class BankRegistry {

    private static long nextTransactionId = 0;

    private int activityCount;

    public static long nextTransactionId() {
        return nextTransactionId(0);
    }

    public void recordActivity() {
        recordActivity(0);
    }

    /**
     * {@code simulatedWorkMillis} exists only so a test can hold the lock long enough to observe contention (or the
     * lack of it) via wall-clock timing — real code has no business sleeping inside a lock. The zero-arg overloads
     * above are what production code actually calls.
     */
    public static synchronized long nextTransactionId(long simulatedWorkMillis) {
        sleepQuietly(simulatedWorkMillis);
        return ++nextTransactionId;
    }

    public synchronized void recordActivity(long simulatedWorkMillis) {
        sleepQuietly(simulatedWorkMillis);
        activityCount++;
    }

    public synchronized int getActivityCount() {
        return activityCount;
    }

    private static void sleepQuietly(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

package com.concurrencybank.testutil;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Asks the JVM's own deadlock detector whether a set of threads is stuck in a cycle. This is what
 * turns "my test timed out, so probably a deadlock" into "the runtime itself reports a circular
 * wait between these two specific threads" — a much stronger claim, and the difference between a
 * test that hangs the build and a test that names the bug.
 *
 * <p><b>Why every method takes the thread ids you care about.</b> {@link
 * ThreadMXBean#findDeadlockedThreads()} is <b>JVM-global</b>: it reports every deadlocked thread in
 * the process, not just yours. Surefire runs all of this module's test classes in a single fork,
 * and {@code NaiveTransferServiceDeadlockTest} deliberately leaves two threads deadlocked forever.
 * A bare {@code assertThat(findDeadlockedThreads()).isNull()} anywhere else would therefore pass or
 * fail depending on class execution order. Scoping every assertion to the calling test's own thread
 * ids is what makes both directions of the claim order-independent.
 *
 * <p>{@code findDeadlockedThreads()} rather than {@code findMonitorDeadlockedThreads()}: the latter
 * only sees cycles built from {@code synchronized} monitors, and {@code LockedAccount} guards
 * itself with a {@link java.util.concurrent.locks.ReentrantLock}. Only the former walks ownable
 * synchronizers too, so only the former can see this repo's deadlock.
 */
public final class DeadlockProbe {

  private static final ThreadMXBean THREADS = ManagementFactory.getThreadMXBean();

  private DeadlockProbe() {}

  /**
   * Currently-deadlocked thread ids, as a set rather than the raw {@code long[]}-or-{@code null}
   * the JMX API returns — an empty set is far easier to assert against than a null.
   */
  public static Set<Long> deadlockedThreadIds() {
    long[] found = THREADS.findDeadlockedThreads();
    return found == null ? Set.of() : Arrays.stream(found).boxed().collect(Collectors.toSet());
  }

  /**
   * Polls until every id in {@code threadIds} appears in the deadlock report.
   *
   * <p>Polling is genuinely the right tool here, unlike elsewhere in this module where a latch
   * would be: the threads being watched are — by construction — never going to reach a rendezvous
   * again, so there is nothing for them to signal with. The only way to learn they are stuck is to
   * ask a third party, repeatedly. There is also a real gap between releasing them and the cycle
   * forming, because each still has to travel from "barrier tripped" to "parked on the second
   * lock".
   *
   * @throws AssertionError if the deadlock has not formed within {@code timeout}
   */
  public static Set<Long> awaitDeadlockAmong(Set<Long> threadIds, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    Set<Long> deadlocked = Set.of();
    while (System.nanoTime() < deadline) {
      deadlocked = deadlockedThreadIds();
      if (deadlocked.containsAll(threadIds)) {
        return deadlocked;
      }
      Thread.sleep(20);
    }
    throw new AssertionError(
        "expected threads "
            + threadIds
            + " to deadlock within "
            + timeout
            + ", but the JVM reported "
            + (deadlocked.isEmpty() ? "no deadlock at all" : "only " + deadlocked));
  }
}

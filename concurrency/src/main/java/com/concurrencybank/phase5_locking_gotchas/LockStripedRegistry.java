package com.concurrencybank.phase5_locking_gotchas;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The fix for {@link StringLockBugDemo}: hand out one private {@code Object} lock per key, created
 * once via {@code computeIfAbsent} (itself safe under concurrent callers - see {@code
 * ConcurrentLedger} for why). Unlike a string literal, a {@code new Object()} is never interned and
 * never shared with anything else in the JVM, so two callers locking on different keys never
 * interfere with each other - "lock striping".
 */
public class LockStripedRegistry {

  private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

  public void runExclusively(String key, InterruptibleAction action) throws InterruptedException {
    Object lock = locks.computeIfAbsent(key, k -> new Object());
    synchronized (lock) {
      action.run();
    }
  }
}

package com.concurrencybank.phase7_primitives;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ConcurrentLruCacheTest {

  @Test
  void evictsTheOldestEntryOnceCapacityIsReached() {
    ConcurrentLruCache<String, Integer> cache = new ConcurrentLruCache<>(3);

    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);
    cache.put("d", 4);

    assertThat(cache.size()).as("capacity is a hard ceiling").isEqualTo(3);
    assertThat(cache.get("a")).as("a was the oldest and was evicted").isNull();
    assertThat(cache.keysInAccessOrder()).containsExactly("b", "c", "d");
  }

  /**
   * The property that makes this an LR<b>U</b> cache rather than an LR<b>I</b> one, and the reason
   * {@code get} has to take the write lock: reading {@code a} re-links it as the freshest entry,
   * which changes who gets evicted next. With {@code accessOrder=false} this test fails and {@code
   * a} disappears instead of {@code b}.
   */
  @Test
  void readingAnEntryRescuesItFromTheNextEviction() {
    ConcurrentLruCache<String, Integer> cache = new ConcurrentLruCache<>(3);
    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);

    assertThat(cache.get("a")).isEqualTo(1);
    assertThat(cache.keysInAccessOrder())
        .as("the read moved a to the most-recent end")
        .containsExactly("b", "c", "a");

    cache.put("d", 4);

    assertThat(cache.get("b")).as("b became the least recently used").isNull();
    assertThat(cache.get("a")).as("a survived because it had been read").isEqualTo(1);
  }

  @Test
  void concurrentWritersNeverPushItPastCapacity() throws InterruptedException {
    int capacity = 50;
    ConcurrentLruCache<Integer, Integer> cache = new ConcurrentLruCache<>(capacity);
    AtomicInteger nextKey = new AtomicInteger();

    runConcurrently(
        100,
        () -> {
          for (int i = 0; i < 200; i++) {
            int key = nextKey.getAndIncrement();
            cache.put(key, key);
            cache.get(key);
          }
        });

    assertThat(cache.size())
        .as("20,000 writes from 100 threads, still exactly at capacity")
        .isEqualTo(capacity);
    assertThat(cache.keysInAccessOrder()).doesNotHaveDuplicates();
  }
}

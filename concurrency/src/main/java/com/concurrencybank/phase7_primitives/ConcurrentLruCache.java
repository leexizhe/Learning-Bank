package com.concurrencybank.phase7_primitives;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A fixed-capacity cache that evicts the least recently <em>used</em> entry —
 * where "used" counts reads as well as writes. Built the way you'd build it in
 * 45 minutes: {@link LinkedHashMap} in access-order mode does the ordering, and
 * one lock makes it thread-safe.
 *
 * <p><b>The trap, and it is the whole point of this class:</b> in access-order
 * mode {@code LinkedHashMap.get} is a <b>mutating</b> operation. It unlinks the
 * entry and re-appends it at the most-recent end. So the obvious "optimisation"
 * — a {@link java.util.concurrent.locks.ReadWriteLock} with {@code get} taking
 * the read lock — is a <em>bug</em>: read locks are shared, several threads
 * would re-link the same intrusive list at once, and the map's internal
 * structure corrupts. Silently, and only under load. If an interviewer offers
 * you a read-write lock here, the right answer is "not for LRU, because the read
 * path writes".
 *
 * <p>Which is also why this doesn't extend {@link java.util.concurrent.ConcurrentHashMap}
 * and bolt on ordering: CHM's whole value is lock-free reads, and LRU
 * fundamentally needs to record every read somewhere. You cannot have both
 * cheaply. The real caches that manage it (Caffeine, and Guava before it) give
 * up exact LRU — they buffer accesses in per-thread ring buffers and replay them
 * onto the eviction policy asynchronously, so the ordering is approximate and
 * eventually consistent. <b>Exact LRU and lock-free reads are mutually
 * exclusive; pick which one you actually need.</b> That sentence is the senior
 * answer to "now make it concurrent".
 *
 * <p>{@link ReentrantLock} rather than {@code synchronized} so the class can
 * grow a {@code tryLock} variant later without restructuring, and because it's
 * the form you'd want if TTL eviction were added on top — see
 * {@code ConcurrentLruCacheTest} for what's actually asserted.
 */
public class ConcurrentLruCache<K, V> {

    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final LinkedHashMap<K, V> entries;

    public ConcurrentLruCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive, was " + capacity);
        }
        this.capacity = capacity;
        // The third constructor argument is the one that matters: accessOrder=true
        // switches the map from insertion order to access order. With it false
        // this is an LRI cache (least recently *inserted*), which is a different
        // and much less useful thing.
        this.entries = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > ConcurrentLruCache.this.capacity;
            }
        };
    }

    /** Returns {@code null} on a miss, and counts as a use on a hit. */
    public V get(K key) {
        lock.lock();
        try {
            return entries.get(key);
        } finally {
            lock.unlock();
        }
    }

    public void put(K key, V value) {
        lock.lock();
        try {
            entries.put(key, value);
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return entries.size();
        } finally {
            lock.unlock();
        }
    }

    /** Most-recently-used last. Test-facing: eviction order is the property worth asserting. */
    List<K> keysInAccessOrder() {
        lock.lock();
        try {
            return List.copyOf(entries.keySet());
        } finally {
            lock.unlock();
        }
    }
}

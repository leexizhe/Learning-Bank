package com.concurrencybank.phase7_primitives;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link com.concurrencybank.phase6_async_patterns.TellerQueue} rewritten on {@link ReentrantLock} plus two {@link
 * Condition}s. Same data structure, same guarantees, same tests — <b>read the two files side by side</b>, because the
 * diff between them is the entire answer to "now do it with {@code Condition}s", which is the standard follow-up once
 * you've written the {@code wait}/{@code notify} version.
 *
 * <p><b>What actually changes: the thundering herd goes away.</b> {@code TellerQueue} has one intrinsic monitor and
 * therefore one wait set holding two different kinds of waiter — producers blocked on "full", consumers blocked on
 * "empty". {@code notify()} there would be a latent deadlock, because it wakes one arbitrary waiter and the JVM may
 * well pick a producer when only a consumer can make progress; that thread re-checks, finds nothing changed, and goes
 * back to sleep while the waiter that could have proceeded is never woken. So {@code TellerQueue} is forced to use
 * {@code notifyAll()} and wake <em>everyone</em>, every time, so that the right thread is somewhere in the stampede.
 * With N blocked threads that's N wakeups and N lock acquisitions to accomplish one handoff.
 *
 * <p>Two {@code Condition}s on one lock split that single wait set in two. A producer that just added an item signals
 * {@code notEmpty}, which contains <em>only</em> consumers, so exactly one consumer wakes and it is guaranteed to be a
 * thread that can proceed. One wakeup per handoff instead of N. That is the whole reason {@code Condition} exists —
 * <b>{@code notify()} is unusable when one monitor guards two predicates, and {@code Condition} is how you get one
 * monitor per predicate.</b>
 *
 * <p><b>The subtlety most candidates miss:</b> the {@code while} loop is <em>still</em> required. Targeted signalling
 * narrows who gets woken; it does not promise the predicate is still true when the woken thread finally runs. {@code
 * signal()} only moves a thread from the condition queue to the lock queue — it still has to reacquire the lock, and
 * {@code ReentrantLock} is <b>non-fair by default</b>, so a brand-new caller can barge in and take the item first. Add
 * spurious wakeups on top and {@code if} is wrong here for exactly the same reason it is wrong in {@code TellerQueue}.
 * Nothing about {@code Condition} makes {@code while} optional; people assume it does.
 *
 * <p><b>Why {@code lockInterruptibly()} rather than {@code lock()}:</b> a thread parked on a full buffer should be
 * cancellable. {@code lock()} is famously <em>not</em> interruptible — it will sit there accumulating the interrupt
 * flag and never act on it — so using it here would mean a shutdown request could not dislodge a blocked producer. Both
 * methods already declare {@code InterruptedException}; refusing to honour it would be the swallowed-interrupt mistake
 * in a different costume.
 *
 * <p>Note the price paid for all this: explicit {@code lock()}/{@code unlock()} with the unlock in a {@code finally}.
 * {@code synchronized} releases the monitor on any exit path for free, including a thrown exception; here, one missing
 * {@code finally} leaks the lock and wedges every other thread forever. {@code TellerQueue} is genuinely the safer
 * code. This one is the faster and more expressive code. That trade — not "Condition is better" — is the answer.
 */
public class BoundedBufferWithCondition<T> {

    private final Queue<T> items = new ArrayDeque<>();
    private final int capacity;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    public BoundedBufferWithCondition(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive, was " + capacity);
        }
        this.capacity = capacity;
    }

    public void submit(T item) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (items.size() == capacity) {
                notFull.await();
            }
            items.add(item);
            // Signal the consumers' queue only. No producer is woken, because no producer could do anything with "an
            // item arrived" anyway.
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    public T take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (items.isEmpty()) {
                notEmpty.await();
            }
            T item = items.remove();
            notFull.signal();
            return item;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return items.size();
        } finally {
            lock.unlock();
        }
    }
}

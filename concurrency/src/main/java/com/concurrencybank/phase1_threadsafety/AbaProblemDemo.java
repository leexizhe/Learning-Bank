package com.concurrencybank.phase1_threadsafety;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * The follow-up to {@link AtomicAccount}'s CAS retry loop: <b>"when does CAS give you the wrong answer even though it
 * succeeded?"</b>
 *
 * <p>CAS asks "is the value still what I read?" — which is not the same question as "has nothing happened since I
 * read?". If another thread changes A to B and back to A, the comparison passes and the swap goes through, even though
 * the world the decision was based on came and went in between. That is the <b>ABA problem</b>.
 *
 * <p><b>When it is harmless.</b> {@link AtomicAccount} holds a {@code long} balance, and a balance of 100 is a balance
 * of 100 regardless of how it got back there. The CAS is guarding an arithmetic invariant, and arithmetic doesn't care
 * about history — which is why that class needs no stamp and shouldn't have one.
 *
 * <p><b>When it corrupts memory.</b> Swap the {@code long} for a <em>reference</em> and the identical CAS becomes a
 * use-after-free. The textbook case is a lock-free stack: thread 1 reads head = node A and prepares to CAS head to
 * A.next; thread 2 pops A, pops B, then pushes A back — but A.next now points at something recycled or freed. Thread
 * 1's CAS sees head == A, succeeds, and installs a stale pointer. The bug is not that the value changed; it is that the
 * value is the same object with different <em>contents</em>.
 *
 * <p><b>The fix</b> is to make the CAS compare identity <em>and</em> a version number. {@link AtomicStampedReference}
 * pairs the reference with an {@code int} stamp incremented on every write, so a value that left and came back no
 * longer compares equal. {@link java.util.concurrent.atomic.AtomicMarkableReference} is the cheaper cousin when a
 * single bit is enough — "is this node logically deleted?" — as used in lock-free linked lists.
 *
 * <p>The stamp is an {@code int}, so it can in principle wrap around and resurrect the problem. In practice 2^32
 * modifications between one thread's read and its CAS is not a scenario worth engineering against, but knowing the
 * guarantee is bounded rather than absolute is the honest version of the answer.
 *
 * <p>Note this needs no threads to demonstrate: ABA is about the <em>sequence of values</em>, not about timing, so
 * {@code AbaProblemDemoTest} performs the A -> B -> A sequence on one thread and the CAS still can't tell.
 */
public class AbaProblemDemo {

    private final AtomicReference<String> unstamped;
    private final AtomicStampedReference<String> stamped;

    public AbaProblemDemo(String initial) {
        this.unstamped = new AtomicReference<>(initial);
        this.stamped = new AtomicStampedReference<>(initial, 0);
    }

    /** What a caller reads before deciding what to CAS to. */
    public String read() {
        return unstamped.get();
    }

    /**
     * @param stampHolder single-element array the current stamp is written into — the {@link AtomicStampedReference}
     *     API's way of returning two values at once, which is ugly but avoids allocating a pair on every read
     */
    public String readStamped(int[] stampHolder) {
        return stamped.get(stampHolder);
    }

    /** An intervening writer. Moves both representations so the two CAS attempts stay comparable. */
    public void set(String value) {
        unstamped.set(value);
        stamped.set(value, stamped.getStamp() + 1);
    }

    /** Succeeds whenever the value matches, however many times it changed in between. */
    public boolean compareAndSet(String expected, String update) {
        return unstamped.compareAndSet(expected, update);
    }

    /** Succeeds only if the value matches <em>and</em> nothing has been written since it was read. */
    public boolean compareAndSetStamped(String expected, int expectedStamp, String update) {
        return stamped.compareAndSet(expected, update, expectedStamp, expectedStamp + 1);
    }
}

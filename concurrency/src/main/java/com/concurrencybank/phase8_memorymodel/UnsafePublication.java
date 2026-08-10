package com.concurrencybank.phase8_memorymodel;

/**
 * Unsafe publication: an object handed to another thread through a plain, non-{@code volatile} field, whose own field
 * is non-{@code final}. The Java Memory Model permits the reader to see the reference but <b>not</b> the write that
 * initialised the object behind it — so {@link Holder#value} can read back as {@code 0} even though no code ever wrote
 * {@code 0}.
 *
 * <p><b>Why that's allowed.</b> A constructor is not atomic and carries no ordering by itself. Writing {@code holder =
 * new Holder(42)} is really three steps — allocate, write {@code value = 42}, publish the reference — and nothing in
 * the JMM forces step two to become visible to another thread before step three. The compiler may reorder them, the
 * CPU's store buffer may drain them out of order, and there is no happens-before edge between the writer and the reader
 * to forbid either. The reader's own read of {@code value} may even have been hoisted before its read of the reference.
 *
 * <p><b>The honest part, and the actual lesson.</b> Its sibling {@link FinalFieldFreeze} is safe from this, and the
 * test can assert that absolutely. This class is <em>permitted</em> to break, but on x86 — whose TSO memory model
 * forbids store-store reordering in hardware — HotSpot will usually not produce the anomaly at all. So {@code
 * Phase8MemoryModelTest.UnsafePublicationTests} reports how often it saw it and asserts only that the harness ran,
 * while the final-field version asserts the guarantee outright.
 *
 * <p>That asymmetry is the point worth taking into an interview: <b>you cannot test your way to memory-model
 * correctness.</b> A green suite proves your hardware declined to demonstrate the bug today, not that the bug isn't
 * there — and the same code on an ARM server, where store-store reordering is permitted and does happen, is a different
 * story. Memory-model claims are reasoned about from the specification, or checked with a tool built for it (jcstress),
 * never inferred from a passing stress loop.
 *
 * <p>Fixes, any one of which is sufficient: make {@code value} {@code final} (see {@link FinalFieldFreeze}), make the
 * publishing field {@code volatile}, publish under a lock held by both threads, or hand the reference over through
 * something that already establishes happens-before — a {@code BlockingQueue}, an {@code AtomicReference}, {@code
 * Thread.start()}.
 */
public class UnsafePublication {

    /** Plain field. No {@code volatile}, so publishing it creates no happens-before edge. */
    private Holder holder;

    public void publish(int value) {
        holder = new Holder(value);
    }

    /** May return {@code null} (not yet published) or a Holder whose value is not yet visible. */
    public Holder read() {
        return holder;
    }

    public void reset() {
        holder = null;
    }

    /**
     * Deliberately non-final — that single keyword is the whole difference from {@link FinalFieldFreeze}.
     */
    public static final class Holder {

        private int value;

        Holder(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }
}

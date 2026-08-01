package com.concurrencybank.phase8_memorymodel;

/**
 * The same unsafe publication as {@link UnsafePublication} — plain field, no {@code volatile}, no
 * lock — with exactly one character changed: the holder's field is {@code final}. That one keyword
 * makes the anomaly <b>impossible</b> rather than merely unlikely, and {@code FinalFieldFreezeTest}
 * asserts it outright.
 *
 * <p><b>The final-field freeze.</b> The JMM gives {@code final} fields a special guarantee that no
 * other field has: a freeze action occurs at the end of the constructor, and any thread that reads
 * the object through a reference it obtained <em>after</em> construction finished is guaranteed to
 * see the correctly initialised value. The JVM implements it by emitting a store-store barrier
 * before the constructor returns, so the field write can never be reordered past the reference
 * publication. No synchronization, no {@code volatile}, no cost on the read side.
 *
 * <p>This is why immutable objects are safe to share without any synchronization at all, and why
 * "make it immutable" is a real concurrency strategy rather than just tidy design — it's also why
 * {@link String} and the boxed primitives can be passed between threads freely.
 *
 * <p><b>The two conditions people forget:</b>
 *
 * <ul>
 *   <li>The guarantee covers only what is reachable through {@code final} fields. A {@code final}
 *       field pointing at a mutable object protects the <em>reference</em>, not the object's
 *       contents — {@code final List} still needs its own synchronization or an unmodifiable copy.
 *   <li>It is void if {@code this} escapes the constructor. Register a listener, start a thread, or
 *       hand {@code this} to anything before the constructor returns, and another thread can
 *       observe the object before the freeze — at which point {@code final} guarantees nothing.
 *       That is the real reason "never let {@code this} escape from a constructor" is a rule.
 * </ul>
 *
 * <p>"Why is a {@code final} field visible without synchronization when a non-{@code final} one
 * isn't?" is a standard senior probe, and these two classes exist so the answer can be pointed at
 * rather than recited.
 */
public class FinalFieldFreeze {

  /** Still a plain field. The safety comes entirely from {@link Holder#value} being final. */
  private Holder holder;

  public void publish(int value) {
    holder = new Holder(value);
  }

  /** May return {@code null} (not yet published), but never a partially-initialised Holder. */
  public Holder read() {
    return holder;
  }

  public void reset() {
    holder = null;
  }

  public static final class Holder {

    private final int value;

    Holder(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }
}

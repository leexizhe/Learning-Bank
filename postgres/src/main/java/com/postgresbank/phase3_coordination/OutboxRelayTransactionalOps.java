package com.postgresbank.phase3_coordination;

import com.postgresbank.common.Outbox;
import com.postgresbank.common.OutboxRepository;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The transactional half of the outbox relay, split out from {@link OutboxRelay} for a reason that
 * is worth more than the code: <b>this used to be a live bug in this repo.</b>
 *
 * <p>{@code OutboxRelay} previously held both the {@code @Scheduled poll()} and the
 * {@code @Transactional relayOnce()}, with the former calling the latter on {@code this}.
 * {@code @Transactional} is implemented by a proxy that wraps the bean, and a proxy can only
 * intercept calls arriving from <em>outside</em> the object — an internal call goes straight to the
 * target and the annotation does nothing at all. So the scheduled path opened no transaction,
 * {@code findByPublishedFalse()} returned entities detached from Spring Data's own short read-only
 * transaction, and {@code setPublished(true)} on a detached entity was never flushed. The relay
 * logged "publishing…" every two seconds, forever, and marked nothing.
 *
 * <p><b>And the test suite did not notice</b>, which is the part worth sitting with. {@code
 * OutboxIT} called {@code relayOnce()} directly — an <em>external</em> call, so it went through the
 * proxy and worked perfectly. The suite exercised the one path that was fine and never touched the
 * one that ran in production. A green build proved the method worked when called the way the test
 * called it, which was not the way the application called it.
 *
 * <p>Doubly worth knowing because this module already gets this right elsewhere: {@code
 * JointOverdraftTransactionalOps} and {@code TransferTransactionalOps} exist for exactly this
 * reason. The pattern was applied everywhere except the one class where the failure would be
 * silent.
 *
 * <p>The general rule: <b>self-invocation defeats every Spring proxy</b>, not just
 * {@code @Transactional} — {@code @Async}, {@code @Cacheable} and {@code @PreAuthorize} all fail
 * the same way. The fix is always to move the annotated method onto a different bean, as here.
 * Self-injection or {@code AopContext.currentProxy()} both work and both leave the next reader
 * wondering why.
 */
@Slf4j
@Component
public class OutboxRelayTransactionalOps {

  /**
   * Bounded rather than unbounded. A relay that has fallen behind should catch up in batches
   * instead of loading the whole backlog into one transaction and timing out at exactly the moment
   * it is most needed. The partial index on {@code (id) WHERE NOT published} is what keeps this
   * claim cheap.
   */
  private static final Limit BATCH = Limit.of(100);

  private final OutboxRepository outbox;

  public OutboxRelayTransactionalOps(OutboxRepository outbox) {
    this.outbox = outbox;
  }

  /**
   * Logging stands in for the publish call — see {@code kafka-bank}'s {@code PaymentOutboxRelayOps}
   * for the same pattern actually wired to a broker. The point of <em>this</em> module is the
   * write-side atomicity guarantee (see {@code phase2_ledger.TransferTransactionalOps}), not the
   * transport.
   *
   * @return how many rows were published on this pass
   */
  @Transactional
  public int relayOnce() {
    List<Outbox> pending = outbox.findByPublishedFalseOrderById(BATCH);
    for (Outbox event : pending) {
      log.info("publishing outbox event {}: {}", event.getEventId(), event.getPayload());
      event.setPublished(true);
    }
    return pending.size();
  }
}

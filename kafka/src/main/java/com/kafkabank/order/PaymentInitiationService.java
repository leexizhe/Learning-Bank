package com.kafkabank.order;

import com.kafkabank.common.PaymentInitiated;
import com.kafkabank.common.Topics;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

/**
 * The producer. This is the "Order Service" of the three-service story: it validates nothing about
 * balances, takes no locks, and touches no ledger — it just records that a customer asked for a
 * payment, durably, and gets out of the way.
 *
 * <p>That's the architectural point worth saying out loud: the synchronous alternative is Order →
 * Payment → Notification → Fraud as a chain of blocking calls, where total latency is the sum of
 * all four and any one of them being down fails the customer's request. Here the API call finishes
 * as soon as Kafka has the event, and every downstream consumer works in parallel.
 */
@Slf4j
@Service
public class PaymentInitiationService {

  /**
   * Bounds how long a request thread will wait for the broker to acknowledge. Without an explicit
   * timeout, {@code Future.get()} waits up to the producer's {@code delivery.timeout.ms} — two
   * minutes by default. An unreachable broker would then park every incoming request thread for two
   * minutes each, exhaust the servlet pool, and take down endpoints that have nothing to do with
   * payments. Failing this one request in 10 seconds is much better behaviour.
   */
  private static final Duration PUBLISH_TIMEOUT = Duration.ofSeconds(10);

  private final KafkaTemplate<String, Object> kafkaTemplate;

  public PaymentInitiationService(KafkaTemplate<String, Object> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public InitiatePaymentResponse initiate(Long accountId, long amountMinor, String description) {
    String paymentId = UUID.randomUUID().toString();
    String eventId = UUID.randomUUID().toString();
    PaymentInitiated event =
        new PaymentInitiated(eventId, paymentId, accountId, amountMinor, description);

    // THE KEY DECISION: accountId as the message key.
    //
    // Kafka assigns a partition as hash(key) % partitionCount, so every event for
    // account 42 lands on the same partition. Ordering in Kafka is only guaranteed
    // *within* a partition, so this is what makes "deposit then withdraw" arrive in
    // that order for a given account. Key on paymentId instead and two events for
    // the same account can land on different partitions, be consumed by different
    // threads, and apply out of order.
    //
    // The trade-off: a very hot account concentrates load on one partition, and one
    // slow record blocks everything behind it on that partition (head-of-line
    // blocking). Ordering per key and even load distribution are in direct tension.
    String key = String.valueOf(accountId);

    try {
      SendResult<String, Object> result =
          kafkaTemplate
              .send(Topics.PAYMENT_EVENTS, key, event)
              .get(PUBLISH_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      var metadata = result.getRecordMetadata();
      log.info(
          "Published PaymentInitiated paymentId={} key={} -> partition={} offset={}",
          paymentId,
          key,
          metadata.partition(),
          metadata.offset());
      return new InitiatePaymentResponse(
          paymentId, eventId, metadata.partition(), metadata.offset());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while publishing payment " + paymentId, e);
    } catch (ExecutionException e) {
      // Waiting for the broker's acknowledgement is deliberate: if we can't durably
      // record the customer's request, the honest answer is an error response, not a
      // 202 that quietly dropped their payment. Fire-and-forget send() would return
      // success to the customer even when the broker never got the record.
      //
      // Waiting on the request thread to get that is a real cost, though. The
      // scalable version returns CompletableFuture<InitiatePaymentResponse> from the
      // controller and lets Spring MVC release the thread until the send completes -
      // same durability guarantee, no thread parked. Kept blocking here because the
      // sequencing is easier to read, but bounded (above) so a sick broker degrades
      // this one endpoint instead of the whole application.
      throw new PaymentPublishException(paymentId, e.getCause());
    } catch (TimeoutException e) {
      throw new PaymentPublishException(paymentId, e);
    }
  }
}

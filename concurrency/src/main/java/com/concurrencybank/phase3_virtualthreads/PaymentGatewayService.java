package com.concurrencybank.phase3_virtualthreads;

import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Joiner;
import java.util.concurrent.StructuredTaskScope.Subtask;
import org.springframework.stereotype.Service;

/**
 * Validates a payment by calling three independent, slow checks (fraud, credit, sanctions)
 * concurrently instead of one after another.
 *
 * <p>{@code StructuredTaskScope} treats the three forked calls as a single unit of work: {@code
 * scope.join()} doesn't return until all three finish, and if any one of them throws, the joiner
 * cancels the rest and propagates the failure — no orphaned virtual thread keeps running after the
 * method returns, which is the resource-leak problem structured concurrency exists to prevent. Each
 * fork runs on its own virtual thread by default, so the three {@code Thread.sleep} calls overlap:
 * wall-clock time is roughly the slowest single check, not the sum of all three.
 */
@Service
public class PaymentGatewayService {

  private static final int POOL_SIZE = 6;

  private final FraudCheckClient fraudCheckClient;
  private final CreditCheckClient creditCheckClient;
  private final SanctionsCheckClient sanctionsCheckClient;
  private final ExecutorService executorService = Executors.newFixedThreadPool(POOL_SIZE);
  // Deliberately a separate pool from executorService above: Q20's "isolate
  // different types of workloads with their own executor" made concrete -
  // this method's async chain never competes with validateWithExecutorService's
  // work for the same threads.
  private final ExecutorService completableFutureExecutor = Executors.newFixedThreadPool(3);

  public PaymentGatewayService(
      FraudCheckClient fraudCheckClient,
      CreditCheckClient creditCheckClient,
      SanctionsCheckClient sanctionsCheckClient) {
    this.fraudCheckClient = fraudCheckClient;
    this.creditCheckClient = creditCheckClient;
    this.sanctionsCheckClient = sanctionsCheckClient;
  }

  public GatewayDecision validate(String transactionId) {
    try (var scope = StructuredTaskScope.open(Joiner.<ValidationResult>allSuccessfulOrThrow())) {
      Subtask<ValidationResult> fraud = scope.fork(() -> fraudCheckClient.check(transactionId));
      Subtask<ValidationResult> credit = scope.fork(() -> creditCheckClient.check(transactionId));
      Subtask<ValidationResult> sanctions =
          scope.fork(() -> sanctionsCheckClient.check(transactionId));

      scope.join();

      List<ValidationResult> results = List.of(fraud.get(), credit.get(), sanctions.get());
      boolean approved = results.stream().allMatch(ValidationResult::approved);
      return new GatewayDecision(transactionId, approved, results);
    } catch (StructuredTaskScope.FailedException e) {
      throw new PaymentValidationException(transactionId, e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while validating " + transactionId, e);
    }
  }

  /**
   * The pre-Java-21 way to get the same fan-out. {@code StructuredTaskScope} is still a preview API
   * in JDK 25 (JEP 505) — some production environments won't turn preview features on, so this is
   * the fallback worth knowing cold.
   *
   * <p>"Normal thread management": a shared {@link ExecutorService} backed by a small, fixed pool
   * of <b>platform</b> threads, created once for the whole service (not per call — a fresh pool per
   * {@code validate()} call would mean unbounded platform threads under load, which defeats the
   * point of bounding it at all). Submitting three tasks to it still runs them concurrently, so
   * wall-clock time is the same as the {@code StructuredTaskScope} version — fan-out was never the
   * hard part.
   *
   * <p>What you lose versus structured concurrency, and have to do by hand:
   *
   * <ul>
   *   <li><b>Cancellation</b> — {@code Future.cancel(true)} has to be called explicitly on the
   *       surviving futures when one fails; forget it and the slow checks keep running to
   *       completion anyway, wasting work.
   *   <li><b>Lifecycle</b> — the pool has to be shut down somewhere ({@link #shutdown()}, wired to
   *       {@code @PreDestroy}) or its threads outlive the application context. {@code
   *       StructuredTaskScope}'s try-with-resources makes this structurally impossible to forget.
   *   <li><b>Sizing</b> — {@code newFixedThreadPool(POOL_SIZE)} is a real capacity-planning number
   *       (how many concurrent {@code validate()} calls do we expect, times 3 checks each?) that
   *       has to be tuned and re-tuned. Virtual threads remove that whole category of decision.
   * </ul>
   *
   * <p><b>Gotcha that's easy to get subtly wrong:</b> calling {@code get()} on the three {@link
   * Future}s in submission order ({@code fraud}, then {@code credit}, then {@code sanctions}) is
   * <i>not</i> fail-fast. If {@code sanctions} fails in 30ms but {@code fraud} takes 2s, a plain
   * submission-order loop still blocks on {@code fraud.get()} first and won't even look at {@code
   * sanctions} until 2s have passed — the opposite of what we want. {@link
   * ExecutorCompletionService} fixes this: {@code take()} returns whichever task finishes next, in
   * completion order, so a failure is seen (and the rest cancelled) as soon as it actually happens.
   */
  public GatewayDecision validateWithExecutorService(String transactionId) {
    CompletionService<ValidationResult> completionService =
        new ExecutorCompletionService<>(executorService);
    List<Future<ValidationResult>> futures =
        List.of(
            completionService.submit(() -> fraudCheckClient.check(transactionId)),
            completionService.submit(() -> creditCheckClient.check(transactionId)),
            completionService.submit(() -> sanctionsCheckClient.check(transactionId)));

    try {
      List<ValidationResult> results = new ArrayList<>();
      for (int i = 0; i < futures.size(); i++) {
        results.add(completionService.take().get());
      }
      boolean approved = results.stream().allMatch(ValidationResult::approved);
      return new GatewayDecision(transactionId, approved, results);
    } catch (ExecutionException e) {
      // Structured concurrency does this cancellation automatically;
      // here it's on us to remember it.
      futures.forEach(future -> future.cancel(true));
      throw new PaymentValidationException(transactionId, e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      futures.forEach(future -> future.cancel(true));
      throw new IllegalStateException("Interrupted while validating " + transactionId, e);
    }
  }

  /**
   * A third variant, using {@code CompletableFuture} on its own dedicated executor — the
   * composition style most Java codebases already reach for day to day, so it's worth seeing next
   * to the other two.
   *
   * <p><b>Never block inside the async chain</b> (Q20): the one blocking call anywhere in this
   * method is {@code Thread.sleep} inside the check clients, standing in for a real HTTP call — in
   * production that would be a genuinely non-blocking HTTP client, not a blocking one wrapped in
   * {@code supplyAsync} and hoped away. Wrapping blocking I/O in {@code supplyAsync} still ties up
   * one of {@code completableFutureExecutor}'s threads for the whole call; it doesn't make the call
   * non-blocking, it just moves which thread blocks.
   *
   * <p><b>Cancellation isn't automatic here — unlike {@link #validate} — but it can be built:</b>
   * {@code CompletableFuture.allOf(...)} on its own does not cancel the other futures when one
   * fails; every future here gets a {@code whenComplete} callback that cancels its siblings the
   * moment any one of them completes exceptionally, which is what makes this fail-fast too.
   *
   * <p><b>The sharp edge worth knowing cold:</b> {@code CompletableFuture .cancel(true)} — unlike
   * {@code Future.cancel(true)} from {@link #validateWithExecutorService} — never interrupts the
   * running task. Its javadoc says so explicitly: "this value has no effect in this implementation
   * because interrupts are not used to control processing." Cancelling {@code fraud}/{@code credit}
   * here unblocks <em>this method</em> quickly (the cancelled futures immediately count as "done"
   * for {@code allOf}), but the underlying {@code Thread.sleep} calls keep running to completion in
   * the background regardless, occupying two of {@code completableFutureExecutor}'s three threads
   * for the full duration. Fast return, real thread leak — the opposite trade-off from {@code
   * validateWithExecutorService}, where {@code Future.cancel(true)} genuinely interrupts the sleep.
   */
  public GatewayDecision validateWithCompletableFuture(String transactionId) {
    CompletableFuture<ValidationResult> fraud =
        CompletableFuture.supplyAsync(
            () -> callUnchecked(() -> fraudCheckClient.check(transactionId)),
            completableFutureExecutor);
    CompletableFuture<ValidationResult> credit =
        CompletableFuture.supplyAsync(
            () -> callUnchecked(() -> creditCheckClient.check(transactionId)),
            completableFutureExecutor);
    CompletableFuture<ValidationResult> sanctions =
        CompletableFuture.supplyAsync(
            () -> callUnchecked(() -> sanctionsCheckClient.check(transactionId)),
            completableFutureExecutor);

    List<CompletableFuture<ValidationResult>> all = List.of(fraud, credit, sanctions);
    all.forEach(
        future ->
            future.whenComplete(
                (result, error) -> {
                  if (error != null) {
                    all.forEach(sibling -> sibling.cancel(true));
                  }
                }));

    // Swallow whatever allOf() reports here - with three futures racing to
    // cancel each other, which exception "wins" the internal combiner tree
    // isn't guaranteed. findRealFailure() below inspects each future
    // directly afterward for the one genuine (non-cancellation) failure,
    // which is deterministic.
    CompletableFuture.allOf(fraud, credit, sanctions).exceptionally(ignored -> null).join();

    Throwable realFailure = findRealFailure(all);
    if (realFailure != null) {
      throw new PaymentValidationException(transactionId, realFailure);
    }

    List<ValidationResult> results = List.of(fraud.join(), credit.join(), sanctions.join());
    boolean approved = results.stream().allMatch(ValidationResult::approved);
    return new GatewayDecision(transactionId, approved, results);
  }

  /**
   * The one future (if any) that failed on its own merits, not as a side effect of us cancelling
   * it.
   */
  private static Throwable findRealFailure(List<CompletableFuture<ValidationResult>> futures) {
    for (CompletableFuture<ValidationResult> future : futures) {
      if (future.isCompletedExceptionally() && !future.isCancelled()) {
        try {
          future.join();
        } catch (CompletionException e) {
          return e.getCause();
        }
      }
    }
    return null;
  }

  /**
   * {@code Supplier} (what {@code supplyAsync} takes) can't declare checked exceptions, so the
   * check clients' {@code InterruptedException}/ {@code SanctionsCheckException} get wrapped here.
   * Throwing a {@link CompletionException} specifically (rather than a plain {@code
   * RuntimeException}) means {@code join()} rethrows this exact instance without an extra layer of
   * wrapping around it.
   */
  private static ValidationResult callUnchecked(Callable<ValidationResult> check) {
    try {
      return check.call();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CompletionException(e);
    } catch (Exception e) {
      throw new CompletionException(e);
    }
  }

  @PreDestroy
  public void shutdown() {
    executorService.shutdown();
    completableFutureExecutor.shutdown();
  }
}

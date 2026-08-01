package com.postgresbank.phase1_isolation;

import static com.postgresbank.testsupport.TestSupport.openAccount;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import com.postgresbank.common.LedgerService;
import com.postgresbank.common.Posting;
import com.postgresbank.common.PostingRepository;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * The write-skew anomaly, made concrete: two accounts share one combined overdraft limit. A
 * withdrawal from either account is only allowed if the <em>combined</em> balance can cover it. Two
 * concurrent withdrawals - one per account - can each individually pass that check before either
 * has committed, because neither transaction wrote a row the other's read depended on directly; the
 * conflict is between one transaction's read and the other's write, which READ COMMITTED never
 * detects.
 *
 * <p>Both scenarios force the two transactions to finish their read (and thus their check) before
 * either proceeds to write, using a {@link CountDownLatch} passed in as the {@code afterRead} hook
 * - without that, the anomaly would only reproduce "most of the time," which is a bad property for
 * a test (and a worse one for a bank).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WriteSkewIT extends TestContainerConfig {

  @Autowired private AccountRepository accounts;

  @Autowired private PostingRepository postings;

  @Autowired private LedgerService ledger;

  @Autowired private JointOverdraftService overdraft;

  private final ExecutorService pool = Executors.newFixedThreadPool(2);

  @Test
  void readCommittedAllowsWriteSkew() throws Exception {
    Account a = openAccount(accounts);
    Account b = openAccount(accounts);
    postings.save(new Posting(a, null, 100, "seed"));

    CountDownLatch bothRead = new CountDownLatch(2);
    Runnable afterRead = rendezvous(bothRead);

    Future<Boolean> first = pool.submit(withdraw(a, b, 80, afterRead, false));
    Future<Boolean> second = pool.submit(withdraw(b, a, 80, afterRead, false));

    boolean firstOk = first.get(10, TimeUnit.SECONDS);
    boolean secondOk = second.get(10, TimeUnit.SECONDS);

    assertThat(firstOk).isTrue();
    assertThat(secondOk).isTrue();
    long combined = ledger.balanceOf(a.getId()) + ledger.balanceOf(b.getId());
    assertThat(combined)
        .as("both withdrawals succeeded under READ COMMITTED - the combined balance went negative")
        .isEqualTo(100 - 80 - 80);
  }

  @Test
  void serializableRejectsOneOfTheTwo() throws Exception {
    Account a = openAccount(accounts);
    Account b = openAccount(accounts);
    postings.save(new Posting(a, null, 100, "seed"));

    CountDownLatch bothRead = new CountDownLatch(2);
    Runnable afterRead = rendezvous(bothRead);

    Future<Boolean> first = pool.submit(withdraw(a, b, 80, afterRead, true));
    Future<Boolean> second = pool.submit(withdraw(b, a, 80, afterRead, true));

    List<Boolean> results =
        List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));

    assertThat(results)
        .as("exactly one of the two withdrawals should succeed")
        .containsExactlyInAnyOrder(true, false);
    long combined = ledger.balanceOf(a.getId()) + ledger.balanceOf(b.getId());
    assertThat(combined)
        .as("SERIALIZABLE must never let the combined balance go negative")
        .isEqualTo(100 - 80);
  }

  private Callable<Boolean> withdraw(
      Account debit, Account partner, long amountMinor, Runnable afterRead, boolean serializable) {
    return () -> {
      try {
        if (serializable) {
          overdraft.withdrawSerializable(debit.getId(), partner.getId(), amountMinor, afterRead);
        } else {
          overdraft.withdrawReadCommitted(debit.getId(), partner.getId(), amountMinor, afterRead);
        }
        return true;
      } catch (InsufficientOverdraftException businessRejection) {
        return false;
      }
    };
  }

  /**
   * First two calls rendezvous; every call after that is a no-op (later SERIALIZABLE retries don't
   * need it).
   */
  private Runnable rendezvous(CountDownLatch bothRead) {
    return () -> {
      bothRead.countDown();
      try {
        bothRead.await(2, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
  }
}

package com.postgresbank.phase6_operations;

import static com.postgresbank.testsupport.ExplainSupport.explain;
import static com.postgresbank.testsupport.TestSupport.openAccount;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import com.postgresbank.common.LedgerService;
import com.postgresbank.common.Posting;
import com.postgresbank.common.PostingRepository;
import com.postgresbank.testsupport.ExplainSupport.QueryPlan;
import com.postgresbank.testsupport.IndexFixture;
import java.util.List;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * The snapshot must be <b>invisible</b>: every assertion here compares it against {@code
 * LedgerService.balanceOf}, the full {@code SUM(postings)} it is meant to accelerate. A cache that
 * returns a different answer from the thing it caches is not an optimization, it is a bug, and in a
 * ledger it is a bug that loses money.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SnapshotIT extends TestContainerConfig {

  @Autowired private AccountRepository accounts;

  @Autowired private PostingRepository postings;

  @Autowired private LedgerService ledger;

  @Autowired private BalanceSnapshotService snapshotService;

  @Autowired private DataSource dataSource;

  @Test
  void theSnapshotAgreesWithTheFullSumBeforeDuringAndAfterBeingTaken() {
    Account account = openAccount(accounts);
    post(account, 100, 250, -75);

    assertThat(snapshotService.balanceOf(account.getId()))
        .as("with no snapshot yet it falls back to the full sum, so callers never need to know")
        .isEqualTo(ledger.balanceOf(account.getId()));

    long snapshotted = snapshotService.takeSnapshot(account.getId());
    assertThat(snapshotted).isEqualTo(275);
    assertThat(snapshotService.balanceOf(account.getId()))
        .as("immediately after the snapshot the delta is empty")
        .isEqualTo(ledger.balanceOf(account.getId()));

    post(account, 1_000, -500);

    assertThat(snapshotService.balanceOf(account.getId()))
        .as("snapshot + only the postings written since it was taken")
        .isEqualTo(ledger.balanceOf(account.getId()))
        .isEqualTo(775);

    // Re-taking it must be an upsert, not a second row - the primary key is
    // the account, so a duplicate insert would fail outright.
    snapshotService.takeSnapshot(account.getId());
    assertThat(snapshotService.balanceOf(account.getId())).isEqualTo(775);
  }

  /**
   * The point of the whole exercise, measured rather than asserted from theory: against the
   * 100k-posting fixture account, reading the balance through a snapshot touches a fraction of the
   * buffers that summing the journal does.
   */
  @Test
  void snapshottingCollapsesTheWorkOfReadingALargeAccountsBalance() throws Exception {
    IndexFixture.Fixture fixture = IndexFixture.seedOnce(dataSource);
    long accountId = fixture.bulkAccountId();

    QueryPlan fullSum =
        explain(
            dataSource,
            "select coalesce(sum(amount_minor), 0) from postings where account_id = ?",
            accountId);

    snapshotService.takeSnapshot(accountId);
    long asOf = highWaterMark(accountId);

    QueryPlan deltaSum =
        explain(
            dataSource,
            "select coalesce(sum(amount_minor), 0) from postings where account_id = ? and id > ?",
            accountId,
            asOf);

    assertThat(deltaSum.sharedBlocks())
        .as(
            "reading only the postings since the checkpoint (%d blocks) instead of all %d of them (%d blocks)",
            deltaSum.sharedBlocks(), IndexFixture.BULK_ROWS, fullSum.sharedBlocks())
        .isLessThan(fullSum.sharedBlocks());
    assertThat(snapshotService.balanceOf(accountId))
        .as("and it is still exactly the same number")
        .isEqualTo(ledger.balanceOf(accountId));
  }

  private void post(Account account, long... amounts) {
    List<Posting> rows =
        IntStream.range(0, amounts.length)
            .mapToObj(i -> new Posting(account, null, amounts[i], "snapshot-it"))
            .toList();
    postings.saveAll(rows);
  }

  private long highWaterMark(long accountId) {
    Long max = postings.maxIdByAccountId(accountId);
    return max == null ? 0L : max;
  }
}

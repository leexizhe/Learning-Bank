package com.postgresbank.phase6_operations;

import com.postgresbank.common.PostingRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The transaction boundary for taking a snapshot, split from {@link BalanceSnapshotService} in the
 * module's usual {@code *TransactionalOps} shape — and for the reason {@code
 * OutboxRelayTransactionalOps} documents at length: an annotated method called on {@code this} is
 * not intercepted, so a scheduled or self-invoking caller would silently run without a transaction.
 */
@Component
public class BalanceSnapshotTransactionalOps {

  private final PostingRepository postings;
  private final BalanceSnapshotRepository snapshots;

  public BalanceSnapshotTransactionalOps(
      PostingRepository postings, BalanceSnapshotRepository snapshots) {
    this.postings = postings;
    this.snapshots = snapshots;
  }

  /**
   * Records the balance as of the newest posting that currently exists.
   *
   * <p><b>The sum is bounded by the mark, and that bound is the whole correctness argument.</b>
   * Under READ COMMITTED each statement takes a fresh snapshot, so a posting can commit between the
   * two reads here. If this summed <em>everything</em> rather than everything {@code <= asOf}, that
   * new posting would be inside the recorded balance while sitting above the recorded id — so the
   * delta read would add it a second time and the cached balance would drift upward, permanently
   * and silently. Summing to an explicit ceiling makes the snapshot and the delta partition the
   * postings exactly, with no overlap and no gap, whatever else commits meanwhile.
   *
   * <p>Snapshots are re-taken for the same account repeatedly, so this is an upsert; see {@link
   * BalanceSnapshot} for why that makes {@code merge()} the right semantics here.
   *
   * @return the snapshotted balance
   */
  @Transactional
  public long takeSnapshot(long accountId) {
    Long highWaterMark = postings.maxIdByAccountId(accountId);
    long asOf = highWaterMark == null ? 0L : highWaterMark;
    long balance = postings.sumAmountByAccountIdUpToId(accountId, asOf);
    snapshots.save(new BalanceSnapshot(accountId, asOf, balance));
    return balance;
  }
}

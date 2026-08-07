package com.postgresbank.phase6_operations;

import com.postgresbank.common.PostingRepository;
import java.util.Optional;
import org.springframework.stereotype.Service;

/**
 * The answer to the question every payments interviewer asks about an append-only ledger:
 * <b>"balance is {@code SUM(postings)} — so how do you read one in a millisecond when the account
 * has ten million of them?"</b>
 *
 * <p>{@code LedgerService.balanceOf} is O(postings for that account). That is exactly right at a
 * hundred rows and unusable at ten million, and the design is not wrong — it is unbounded. The fix
 * is not to abandon the journal for a mutable balance column; it is to add a checkpoint:
 *
 * <pre>
 * balance = snapshot.balance_minor
 *         + SUM(postings WHERE account_id = ? AND id &gt; snapshot.as_of_posting_id)
 * </pre>
 *
 * <p>The read becomes O(postings since the last snapshot), which a periodic job keeps small. <b>The
 * immutable journal stays the source of truth</b> — the snapshot is a cache that can always be
 * recomputed from it and audited against it, which is what makes it safe in a ledger. Nothing is
 * ever mutated, nothing is ever lost, and a bad snapshot is a performance bug rather than a
 * correctness one. That distinction is the point of the whole design.
 *
 * <p><b>The alternative worth naming.</b> You could maintain a cached balance column in the same
 * transaction as each posting — O(1) reads, no job — with a nightly reconciliation asserting {@code
 * cached == SUM(postings)} and alerting on drift. It is faster and it is riskier: every write path
 * must remember to update it, and a missed one is silent corruption rather than a slow query.
 * Snapshots fail safe; cached balances fail wrong.
 *
 * <p>Falls back to the full sum when no snapshot exists yet, so callers never need to know whether
 * one has been taken — which also means the snapshot job can be switched off entirely and the
 * system stays correct, just slower.
 */
@Service
public class BalanceSnapshotService {

    private final PostingRepository postings;
    private final BalanceSnapshotRepository snapshots;
    private final BalanceSnapshotTransactionalOps ops;

    public BalanceSnapshotService(
            PostingRepository postings, BalanceSnapshotRepository snapshots, BalanceSnapshotTransactionalOps ops) {
        this.postings = postings;
        this.snapshots = snapshots;
        this.ops = ops;
    }

    /**
     * Identical to {@code LedgerService.balanceOf}, but reads only the postings since the last checkpoint.
     */
    public long balanceOf(long accountId) {
        Optional<BalanceSnapshot> snapshot = snapshots.findById(accountId);
        long base = snapshot.map(BalanceSnapshot::getBalanceMinor).orElse(0L);
        long since = snapshot.map(BalanceSnapshot::getAsOfPostingId).orElse(0L);
        return base + postings.sumAmountByAccountIdAfterId(accountId, since);
    }

    public long takeSnapshot(long accountId) {
        return ops.takeSnapshot(accountId);
    }
}

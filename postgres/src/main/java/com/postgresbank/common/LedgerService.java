package com.postgresbank.common;

import java.util.List;
import org.springframework.stereotype.Service;

/**
 * The one place a balance is computed. It is never a stored column - always
 * {@code SUM(postings.amount_minor)} for the account, read fresh every time.
 * That's the whole point of an append-only ledger: the "current balance" is a
 * derived view over history, not a mutable fact that can drift from it.
 */
@Service
public class LedgerService {

    private final PostingRepository postings;

    public LedgerService(PostingRepository postings) {
        this.postings = postings;
    }

    public long balanceOf(long accountId) {
        return postings.sumAmountByAccountId(accountId);
    }

    /** Same projection summed across two accounts in one query - used by phase1's joint-overdraft check. */
    public long combinedBalanceOf(long accountIdA, long accountIdB) {
        return postings.sumAmountByAccountIdIn(List.of(accountIdA, accountIdB));
    }
}

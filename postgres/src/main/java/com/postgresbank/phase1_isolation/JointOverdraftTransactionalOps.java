package com.postgresbank.phase1_isolation;

import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import com.postgresbank.common.LedgerService;
import com.postgresbank.common.Posting;
import com.postgresbank.common.PostingRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * The actual write-skew shape: two accounts share one overdraft limit (their combined balance must never go negative),
 * and a withdrawal debits only one of them after checking the <em>combined</em> balance. Two concurrent withdrawals -
 * one against each account - can both read "yes, there's enough combined headroom" before either has committed its
 * debit, and both proceed. Neither transaction ever wrote a row the other read, so a plain row lock (as used in
 * phase2's transfer) can't prevent this - it's an anomaly between the *predicate* each transaction checked and a write
 * it never saw coming from the other side.
 *
 * <p>Isolation level is a static annotation attribute, so it can't be chosen at call time - hence two near-identical
 * methods rather than one parameterized method. {@link JointOverdraftService} is the public API that picks between them
 * and adds the SERIALIZABLE retry loop.
 *
 * <p>{@code afterRead} is a deliberate test seam: it lets an IT force two concurrent transactions to both finish their
 * read before either proceeds to write, which is what makes the anomaly reproduce deterministically instead of
 * "usually, if the timing lines up." Production callers pass a no-op.
 */
@Component
public class JointOverdraftTransactionalOps {

    private final LedgerService ledger;
    private final AccountRepository accounts;
    private final PostingRepository postings;

    public JointOverdraftTransactionalOps(
            LedgerService ledger, AccountRepository accounts, PostingRepository postings) {
        this.ledger = ledger;
        this.accounts = accounts;
        this.postings = postings;
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public void withdrawReadCommitted(
            long debitAccountId, long partnerAccountId, long amountMinor, Runnable afterRead) {
        withdraw(debitAccountId, partnerAccountId, amountMinor, afterRead);
    }

    @Transactional(isolation = Isolation.SERIALIZABLE)
    public void withdrawSerializableOnce(
            long debitAccountId, long partnerAccountId, long amountMinor, Runnable afterRead) {
        withdraw(debitAccountId, partnerAccountId, amountMinor, afterRead);
    }

    private void withdraw(long debitAccountId, long partnerAccountId, long amountMinor, Runnable afterRead) {
        long combined = ledger.combinedBalanceOf(debitAccountId, partnerAccountId);
        afterRead.run();

        if (combined - amountMinor < 0) {
            throw new InsufficientOverdraftException(combined, amountMinor);
        }

        Account debitAccount = accounts.getReferenceById(debitAccountId);
        postings.save(new Posting(debitAccount, null, -amountMinor, "joint-overdraft-withdrawal"));
    }
}

package com.postgresbank.phase2_ledger;

import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import com.postgresbank.common.Outbox;
import com.postgresbank.common.OutboxRepository;
import com.postgresbank.common.Posting;
import com.postgresbank.common.PostingRepository;
import com.postgresbank.common.Transfer;
import com.postgresbank.common.TransferRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * One transaction, three writes: the outbox event, the {@code transfers} row that owns the unique idempotency
 * constraint, and the two postings (debit + credit - double-entry, enforced here by construction rather than a database
 * CHECK). The outbox row is written <em>first</em> on purpose: it's what lets IdempotencyIT / OutboxIT prove the outbox
 * insert is not durable on its own - if the {@code transfers} insert that follows it hits the unique-key violation,
 * this whole method's transaction rolls back and takes the "already inserted" outbox row down with it. Same
 * transaction, no partial state, regardless of write order.
 */
@Component
@RequiredArgsConstructor
public class TransferTransactionalOps {

    private final AccountRepository accounts;
    private final PostingRepository postings;
    private final TransferRepository transfers;
    private final OutboxRepository outbox;

    @Transactional
    public Long apply(String idempotencyKey, long fromAccountId, long toAccountId, long amountMinor) {
        outbox.save(new Outbox("transfer key=%s from=%d to=%d amount=%d"
                .formatted(idempotencyKey, fromAccountId, toAccountId, amountMinor)));

        // IDENTITY generation forces this INSERT to happen immediately (Hibernate can't batch/defer it, since postings
        // below need the generated id) - so a duplicate idempotencyKey fails right here, synchronously.
        Transfer transfer = transfers.save(new Transfer(idempotencyKey, fromAccountId, toAccountId, amountMinor));

        Account from = accounts.getReferenceById(fromAccountId);
        Account to = accounts.getReferenceById(toAccountId);
        postings.save(new Posting(from, transfer.getId(), -amountMinor, "transfer-debit"));
        postings.save(new Posting(to, transfer.getId(), amountMinor, "transfer-credit"));

        return transfer.getId();
    }
}

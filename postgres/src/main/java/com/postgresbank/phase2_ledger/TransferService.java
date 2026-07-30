package com.postgresbank.phase2_ledger;

import com.postgresbank.common.Transfer;
import com.postgresbank.common.TransferRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

/**
 * Deliberately NOT {@code @Transactional} itself - the actual write happens
 * in {@link TransferTransactionalOps#apply}, a separate Spring bean, so that
 * a failed attempt's exception propagates all the way out of that method's
 * transactional proxy before this method catches it. Catching a
 * {@code DataIntegrityViolationException} thrown by <em>this same</em>
 * object's own {@code @Transactional} method wouldn't work - the exception
 * would already have marked the (still-open, self-invoked) transaction
 * rollback-only, and by the time control returned here the transaction
 * boundary would already be gone or in an inconsistent state. Going through
 * a real proxy call keeps the two transactions - the failed insert attempt,
 * and the read-what-already-exists that follows it - genuinely separate.
 */
@Service
public class TransferService {

    private final TransferTransactionalOps ops;
    private final TransferRepository transfers;

    public TransferService(TransferTransactionalOps ops, TransferRepository transfers) {
        this.ops = ops;
        this.transfers = transfers;
    }

    public TransferResult transfer(String idempotencyKey, long fromAccountId, long toAccountId, long amountMinor) {
        try {
            Long transferId = ops.apply(idempotencyKey, fromAccountId, toAccountId, amountMinor);
            return new TransferResult(transferId, false);
        } catch (DataIntegrityViolationException uniqueViolation) {
            // Someone else's attempt with the same key won the race and committed
            // first. That's success, not failure - report the transfer it created.
            Transfer existing = transfers
                    .findByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> uniqueViolation);
            return new TransferResult(existing.getId(), true);
        }
    }
}

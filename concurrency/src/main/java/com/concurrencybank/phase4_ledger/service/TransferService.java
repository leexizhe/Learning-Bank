package com.concurrencybank.phase4_ledger.service;

import com.concurrencybank.phase4_ledger.dto.TransferResponse;
import com.concurrencybank.phase4_ledger.entity.Account;
import com.concurrencybank.phase4_ledger.exception.AccountNotFoundException;
import com.concurrencybank.phase4_ledger.exception.InsufficientFundsException;
import com.concurrencybank.phase4_ledger.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * DB-layer sibling of {@code phase2_deadlock.LockOrderedTransferService}: same lock-ordering principle (always take the
 * lock on the lower account id first), except the lock is a Postgres row lock instead of a {@code ReentrantLock}, and
 * the transaction manager holds it for the whole {@code @Transactional} method instead of a manual try/finally.
 *
 * <p>Two concurrent transfers between the same pair of accounts, in either direction, always request the row locks in
 * the same order — so the second transaction simply blocks on the {@code SELECT ... FOR UPDATE} until the first commits
 * and releases its locks, instead of the two transactions deadlocking on each other's rows.
 */
@Service
@RequiredArgsConstructor
public class TransferService {

    private final AccountRepository accounts;

    @Transactional
    public TransferResponse transfer(Long fromAccountId, Long toAccountId, long amountMinor) {
        boolean fromIsLower = fromAccountId < toAccountId;
        Long firstId = fromIsLower ? fromAccountId : toAccountId;
        Long secondId = fromIsLower ? toAccountId : fromAccountId;

        Account first = accounts.findByIdForUpdate(firstId).orElseThrow(() -> new AccountNotFoundException(firstId));
        Account second = accounts.findByIdForUpdate(secondId).orElseThrow(() -> new AccountNotFoundException(secondId));

        Account from = fromIsLower ? first : second;
        Account to = fromIsLower ? second : first;

        if (from.getBalanceMinor() < amountMinor) {
            throw new InsufficientFundsException(from.getId(), from.getBalanceMinor(), amountMinor);
        }

        from.debit(amountMinor);
        to.credit(amountMinor);
        // No explicit save(): both accounts are managed entities inside this transaction, so Hibernate's dirty checking
        // flushes the changes on commit.

        return new TransferResponse(
                from.getId(), to.getId(), amountMinor, from.getBalanceMinor(), to.getBalanceMinor());
    }
}

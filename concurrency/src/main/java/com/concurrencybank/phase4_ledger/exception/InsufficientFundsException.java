package com.concurrencybank.phase4_ledger.exception;

public class InsufficientFundsException extends RuntimeException {

    public InsufficientFundsException(Long accountId, long balanceMinor, long amountMinor) {
        super("Account " + accountId + " has " + balanceMinor + " but needs " + amountMinor);
    }
}

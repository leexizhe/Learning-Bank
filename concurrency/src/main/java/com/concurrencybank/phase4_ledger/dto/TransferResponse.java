package com.concurrencybank.phase4_ledger.dto;

public record TransferResponse(
        Long fromAccountId, Long toAccountId, long amountMinor, long fromBalanceAfter, long toBalanceAfter) {}

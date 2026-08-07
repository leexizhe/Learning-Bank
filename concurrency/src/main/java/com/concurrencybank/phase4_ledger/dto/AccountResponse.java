package com.concurrencybank.phase4_ledger.dto;

import com.concurrencybank.phase4_ledger.entity.Account;

public record AccountResponse(Long id, String owner, long balanceMinor) {

    public static AccountResponse from(Account account) {
        return new AccountResponse(account.getId(), account.getOwner(), account.getBalanceMinor());
    }
}

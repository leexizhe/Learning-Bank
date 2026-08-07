package com.concurrencybank.phase4_ledger.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.Getter;

/**
 * {@code version} is kept as defense-in-depth (optimistic locking would catch a lost update if two transactions ever
 * touched a row without the explicit row lock below), but the actual concurrency guarantee for transfers comes from
 * {@code TransferService} taking a pessimistic {@code SELECT ... FOR UPDATE} on both accounts, in ascending-id order,
 * before mutating either one.
 */
@Getter
@Entity
@Table(name = "accounts")
public class Account {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String owner;

    @Column(name = "balance_minor", nullable = false)
    private long balanceMinor;

    @Version
    private long version;

    protected Account() {}

    public Account(String owner, long balanceMinor) {
        this.owner = owner;
        this.balanceMinor = balanceMinor;
    }

    public void debit(long amountMinor) {
        this.balanceMinor -= amountMinor;
    }

    public void credit(long amountMinor) {
        this.balanceMinor += amountMinor;
    }
}

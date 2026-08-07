package com.kafkabank.payment.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.Getter;

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

    public boolean canDebit(long amountMinor) {
        return balanceMinor >= amountMinor;
    }

    public void debit(long amountMinor) {
        this.balanceMinor -= amountMinor;
    }
}

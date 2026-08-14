package com.kafkabank.payment.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.Getter;

/**
 * The balance this module's consumer debits. <b>This one keeps a mutable balance column</b>, unlike the postgres
 * module's {@code Account}, where a balance is always {@code SUM(postings)} and never a stored field. That is a
 * deliberate difference in subject rather than a disagreement: this module is about Kafka delivery semantics, and a
 * single mutable number is the smallest thing that makes a double-debit visible.
 *
 * <p><b>Why {@code @Version} is here even though {@code processed_events} already exists.</b> The two defend different
 * failures, and it is worth being able to say which is which. {@code processed_events} stops the <em>same</em> event
 * being applied twice — redelivery after a rebalance, or after a crash between the work and the offset commit. It says
 * nothing about two <em>different</em> events racing. Ordinarily nothing does race: keying by {@code accountId} puts
 * every event for one account on one partition, consumed by one thread. But that guarantee is exactly what phase 4
 * shows breaking under a retry topic, and it disappears entirely for anything reaching these rows from outside the
 * consumer.
 *
 * <p>The row lock in {@code AccountRepository.findByIdForUpdate} is what actually prevents the lost update.
 * {@code @Version} is defence in depth behind it: if some future path ever loaded an account <em>without</em> the
 * {@code FOR UPDATE}, Hibernate's version check would throw at flush rather than let two reads of the same balance
 * both write. Same reasoning as the concurrency module's {@code Account} — pessimistic lock as the mechanism,
 * optimistic version as the tripwire that catches you having bypassed it.
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

    /**
     * Deliberately separate from {@link #debit(long)} so the caller can answer REJECTED — a business outcome that
     * commits and never retries — rather than throwing. "Insufficient funds" is a correct final answer, not a failure;
     * conflating the two is how a dead-letter topic fills up with ordinary declines nobody can triage.
     */
    public boolean canDebit(long amountMinor) {
        return balanceMinor >= amountMinor;
    }

    /**
     * Check-then-act, and safe only because both halves run inside one transaction that already holds this row's
     * {@code FOR UPDATE} lock. Called on its own it would be exactly the race phase 1 of the concurrency module exists
     * to demonstrate.
     */
    public void debit(long amountMinor) {
        this.balanceMinor -= amountMinor;
    }
}

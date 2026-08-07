package com.postgresbank.common;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

/**
 * No {@code balance} column, on purpose - see {@link LedgerService}. The {@code postings} association is LAZY (the JPA
 * default for {@code @OneToMany}) so phase4_performance's N+1 demo has something real to trigger: touching it outside a
 * fetch-joined query issues one extra SELECT per account.
 */
@Getter
@Entity
@Table(name = "accounts")
public class Account {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String owner;

    @Column(name = "opened_at", nullable = false)
    private Instant openedAt;

    @OneToMany(mappedBy = "account", fetch = FetchType.LAZY)
    private List<Posting> postings = new ArrayList<>();

    protected Account() {}

    public Account(String owner) {
        this.owner = owner;
        this.openedAt = Instant.now();
    }
}

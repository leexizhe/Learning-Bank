package com.acrabank.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.time.LocalDate;
import lombok.Getter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * One row per UEN, holding the whole ACRA response plus the handful of fields worth having as real columns.
 *
 * <p>The split is the point. {@code payload} is the source of truth and is stored whole, so ACRA renaming or adding a
 * field never loses data and never needs a migration; the promoted columns are a convenience for the queries you
 * actually run, and every one of them is nullable because a missing field in the response should degrade to a null
 * column, not to a failed ingest.
 *
 * <p>The UEN is the natural primary key - there is no surrogate id, because ACRA already guarantees uniqueness and a
 * generated id would just invite duplicate rows for the same company.
 */
@Getter
@Entity
@Table(name = "business_profile")
public class BusinessProfile {

    @Id
    private String uen;

    @Column(name = "entity_name")
    private String entityName;

    @Column(name = "entity_status")
    private String entityStatus;

    @Column(name = "entity_type")
    private String entityType;

    @Column(name = "registration_date")
    private LocalDate registrationDate;

    // Hibernate 6 maps String + SqlTypes.JSON straight onto Postgres jsonb, so this needs no hypersistence-utils
    // dependency. Note that jsonb is a parsed representation, not the original text: it normalises whitespace and does
    // not preserve key order, so what comes back out is semantically equal to what went in rather than byte-identical.
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", columnDefinition = "jsonb", nullable = false)
    private String payload;

    @Column(name = "fetched_at", nullable = false)
    private Instant fetchedAt;

    protected BusinessProfile() {}

    public BusinessProfile(String uen) {
        this.uen = uen;
    }

    // Public only because ProfileMapper lives in another package now. It is still meant to be called by the mapper and
    // nothing else - the setters this entity deliberately does not have would be the wider hole.
    public void refresh(
            String payload,
            String entityName,
            String entityStatus,
            String entityType,
            LocalDate registrationDate,
            Instant fetchedAt) {
        this.payload = payload;
        this.entityName = entityName;
        this.entityStatus = entityStatus;
        this.entityType = entityType;
        this.registrationDate = registrationDate;
        this.fetchedAt = fetchedAt;
    }
}

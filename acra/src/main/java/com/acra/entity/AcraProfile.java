package com.acra.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/** One row per UEN: the whole ACRA response, plus the name promoted to a column so queries don't need JSON operators. */
@Getter
@Entity
@Table(name = "acra_profile")
public class AcraProfile {

    @Id
    private String uen;

    @Column(name = "entity_name")
    private String entityName;

    // Hibernate 6 maps String + SqlTypes.JSON straight onto Postgres jsonb, so no extra dependency is needed.
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", columnDefinition = "jsonb", nullable = false)
    private String payload;

    @Column(name = "fetched_at", nullable = false)
    private Instant fetchedAt;

    protected AcraProfile() {}

    public AcraProfile(String uen, String entityName, String payload, Instant fetchedAt) {
        this.uen = uen;
        this.entityName = entityName;
        this.payload = payload;
        this.fetchedAt = fetchedAt;
    }
}

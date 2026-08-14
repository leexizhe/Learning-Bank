package com.postgresbank.phase3_coordination;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.Setter;

/**
 * One row of the work queue that {@link JobRunner} claims from with {@code FOR UPDATE SKIP LOCKED}.
 *
 * <p><b>{@code status} is a bare string, and that is a trap worth seeing rather than hiding.</b> The state machine is
 * only two values — {@code PENDING} on construction here, flipped to {@code DONE} by {@link JobRunner#claimNext()} —
 * but the flip is <em>native SQL</em>, not a JPA update, so nothing links the literal in this constructor to the
 * literal in that query. Rename one and the compiler is silent; the queue simply stops draining. An enum with
 * {@code @Enumerated(EnumType.STRING)} would close that gap, at the cost of obscuring the raw SQL that is the whole
 * point of the phase. The tension is real, and naming it is better than pretending the string is fine.
 *
 * <p>The same split is why {@code status} carries a {@code @Setter} that nothing in this package calls: {@code
 * JobRunner} bypasses the persistence context entirely, so a claimed row's in-memory copy — if one is loaded — will
 * happily still say {@code PENDING}. Read jobs back through the repository <em>after</em> the claiming transaction
 * commits, never across it.
 */
@Getter
@Entity
@Table(name = "payment_jobs")
public class PaymentJob {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String payload;

    @Setter
    @Column(nullable = false)
    private String status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected PaymentJob() {}

    public PaymentJob(String payload) {
        this.payload = payload;
        this.status = "PENDING";
        this.createdAt = Instant.now();
    }
}

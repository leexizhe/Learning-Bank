package com.kafkabank.reconciliation.repository;

import com.kafkabank.reconciliation.entity.ReconciliationRecord;
import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ReconciliationRecordRepository extends JpaRepository<ReconciliationRecord, String> {

    /**
     * Creates the row if nobody has yet, and does nothing if someone already did.
     *
     * <p>{@code ON CONFLICT DO NOTHING} is what makes this safe to call from both listener threads at once. The obvious
     * alternative — "read, and insert if absent" — has a window where both threads read nothing and both insert, and
     * the loser gets a constraint violation that poisons its whole transaction (once Postgres raises the error the
     * transaction is dead; you can't just catch it and carry on in the same one). Pushing the race down to a single
     * atomic statement removes the window instead of trying to recover from it.
     */
    @Modifying
    @Query(value = """
                    INSERT INTO reconciliation_records (payment_id, initiated_seen, state, updated_at)
                    VALUES (:paymentId, FALSE, 'PENDING', NOW())
                    ON CONFLICT (payment_id) DO NOTHING
                    """, nativeQuery = true)
    void insertIfAbsent(@Param("paymentId") String paymentId);

    /**
     * {@code SELECT ... FOR UPDATE}. After {@link #insertIfAbsent} the row is guaranteed to exist, so this always finds
     * it — and the lock serialises the two listener threads so their updates to the same payment can't interleave and
     * lose one half of the story.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select r from ReconciliationRecord r where r.paymentId = :paymentId")
    Optional<ReconciliationRecord> findByIdForUpdate(@Param("paymentId") String paymentId);
}

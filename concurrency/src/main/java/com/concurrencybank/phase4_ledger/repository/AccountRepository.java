package com.concurrencybank.phase4_ledger.repository;

import com.concurrencybank.phase4_ledger.entity.Account;
import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface AccountRepository extends JpaRepository<Account, Long> {

    /**
     * {@code SELECT ... FOR UPDATE} — blocks any other transaction trying to lock the same row until this transaction
     * commits or rolls back. This is the database-level equivalent of {@code LockedAccount}'s {@code ReentrantLock} in
     * {@code phase2_deadlock}: same idea (hold an exclusive lock while you read-then-write a balance), different layer.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select a from Account a where a.id = :id")
    Optional<Account> findByIdForUpdate(@Param("id") Long id);
}

package com.kafkabank.payment.repository;

import com.kafkabank.payment.entity.Account;
import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface AccountRepository extends JpaRepository<Account, Long> {

    /**
     * {@code SELECT ... FOR UPDATE}. Keying by accountId already serialises events
     * for one account within a single partition, but the listener runs with
     * {@code concurrency: 3} and a partition can be reassigned during a rebalance,
     * so the row lock is what actually holds the line at the database.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select a from Account a where a.id = :id")
    Optional<Account> findByIdForUpdate(@Param("id") Long id);
}

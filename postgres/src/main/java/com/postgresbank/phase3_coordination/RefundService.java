package com.postgresbank.phase3_coordination;

import jakarta.persistence.EntityManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * {@code pg_try_advisory_xact_lock} takes a lock keyed on an arbitrary
 * {@code bigint} - here, the order id - with no table or row behind it. It's
 * "try": if another session already holds the same key, this returns
 * {@code false} immediately instead of blocking, which is exactly the shape
 * you want for "don't process the same refund twice concurrently, but don't
 * make the loser wait either." Being transaction-scoped ({@code _xact_}),
 * the lock is released automatically on commit or rollback - no matching
 * unlock call to forget, unlike session-level advisory locks.
 *
 * <p>Cheaper than a row lock because there's no row: no table to create, no
 * index, nothing to vacuum. The trade-off is that the "resource" being
 * locked - here, {@code orderId} - is a convention enforced entirely by
 * every caller agreeing to lock on it before touching that order; Postgres
 * doesn't know it means anything.
 *
 * <p>{@code duringHold} is a test seam (see AdvisoryLockIT), same idea as
 * phase1's {@code afterRead} - it lets a test force a second attempt to
 * happen while the first still holds the lock, instead of hoping the timing
 * lines up.
 */
@Service
public class RefundService {

    private final EntityManager em;

    public RefundService(EntityManager em) {
        this.em = em;
    }

    @Transactional
    public boolean tryRefund(long orderId, Runnable duringHold) {
        Boolean acquired = (Boolean)
                em.createNativeQuery("select pg_try_advisory_xact_lock(:orderId)")
                        .setParameter("orderId", orderId)
                        .getSingleResult();

        if (!Boolean.TRUE.equals(acquired)) {
            return false;
        }

        duringHold.run();
        return true;
    }

    public boolean tryRefund(long orderId) {
        return tryRefund(orderId, () -> {});
    }
}

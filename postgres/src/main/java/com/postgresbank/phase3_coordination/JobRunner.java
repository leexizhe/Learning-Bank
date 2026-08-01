package com.postgresbank.phase3_coordination;

import jakarta.persistence.EntityManager;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The scalable-job-queue pattern: many instances of this service (in production, many Pods) can
 * call {@link #claimNext()} concurrently against the same table without colliding. {@code FOR
 * UPDATE} takes a row lock on the candidate row; {@code SKIP LOCKED} means a worker that would
 * otherwise block waiting for a row someone else already claimed instead skips it and looks at the
 * next one. Without {@code SKIP LOCKED}, every worker but one would queue up waiting on the same
 * lock - serializing all your workers instead of parallelizing them. {@code LIMIT 1} bounds the row
 * lock to exactly one job per call.
 *
 * <p>Both the claim and the status flip are plain native SQL, kept out of the JPA persistence
 * context on purpose - the point here is the SQL itself, not a Spring Data lock-mode abstraction
 * over it.
 */
@Service
public class JobRunner {

  private final EntityManager em;

  public JobRunner(EntityManager em) {
    this.em = em;
  }

  @Transactional
  public Optional<Long> claimNext() {
    List<?> rows =
        em.createNativeQuery(
                "select id from payment_jobs where status = :status order by id limit 1 for update skip locked")
            .setParameter("status", "PENDING")
            .getResultList();

    if (rows.isEmpty()) {
      return Optional.empty();
    }

    long id = ((Number) rows.get(0)).longValue();
    em.createNativeQuery("update payment_jobs set status = 'DONE' where id = :id")
        .setParameter("id", id)
        .executeUpdate();
    return Optional.of(id);
  }
}

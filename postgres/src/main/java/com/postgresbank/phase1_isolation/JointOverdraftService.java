package com.postgresbank.phase1_isolation;

import java.sql.SQLException;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

/**
 * Public entry point. {@link #withdrawReadCommitted} is left free to
 * reproduce write skew (that's the point - it's the anomaly demo). {@link
 * #withdrawSerializable} is the fix: Postgres detects the conflict itself and
 * fails one of the two transactions with SQLSTATE {@code 40001} ("could not
 * serialize access due to read/write dependencies"). The contract of
 * SERIALIZABLE was never "no one gets it wrong," it's "if it would be wrong,
 * the database tells you and you retry" - so the retry loop here isn't a
 * workaround, it's the other half of the guarantee.
 *
 * <p>Detection reads the SQLState directly off the exception's cause chain
 * rather than trusting a specific Spring/Hibernate exception subtype - which
 * translated type a given driver/ORM version produces for 40001 has shifted
 * before, but the SQLState itself is Postgres's own contract and won't.
 */
@Service
public class JointOverdraftService {

    private static final String SERIALIZATION_FAILURE_SQLSTATE = "40001";
    private static final int MAX_ATTEMPTS = 10;

    private final JointOverdraftTransactionalOps ops;

    public JointOverdraftService(JointOverdraftTransactionalOps ops) {
        this.ops = ops;
    }

    public void withdrawReadCommitted(long debitAccountId, long partnerAccountId, long amountMinor) {
        withdrawReadCommitted(debitAccountId, partnerAccountId, amountMinor, () -> {});
    }

    public void withdrawReadCommitted(
            long debitAccountId, long partnerAccountId, long amountMinor, Runnable afterRead) {
        ops.withdrawReadCommitted(debitAccountId, partnerAccountId, amountMinor, afterRead);
    }

    public void withdrawSerializable(long debitAccountId, long partnerAccountId, long amountMinor) {
        withdrawSerializable(debitAccountId, partnerAccountId, amountMinor, () -> {});
    }

    public void withdrawSerializable(
            long debitAccountId, long partnerAccountId, long amountMinor, Runnable afterRead) {
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                ops.withdrawSerializableOnce(debitAccountId, partnerAccountId, amountMinor, afterRead);
                return;
            } catch (DataAccessException ex) {
                if (attempt == MAX_ATTEMPTS || !isSerializationFailure(ex)) {
                    throw ex;
                }
                // Retry with fresh reads - this is not the same transaction retrying,
                // it's a brand-new one, exactly as SERIALIZABLE requires.
            }
        }
    }

    private boolean isSerializationFailure(Throwable ex) {
        for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sqlException
                    && SERIALIZATION_FAILURE_SQLSTATE.equals(sqlException.getSQLState())) {
                return true;
            }
        }
        return false;
    }
}

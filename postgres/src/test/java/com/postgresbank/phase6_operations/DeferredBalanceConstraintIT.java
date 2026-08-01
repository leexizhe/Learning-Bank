package com.postgresbank.phase6_operations;

import static com.postgresbank.testsupport.TestSupport.openAccount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * "Double-entry is enforced by construction rather than by a database
 * constraint" invites the obvious follow-up: <b>so how would you enforce it in
 * the database?</b> A plain {@code CHECK} cannot — it sees one row, and the
 * invariant spans the set of rows sharing a {@code transfer_id}. The answer is a
 * <b>deferred constraint trigger</b>.
 *
 * <p><b>Deferred is the entire point.</b> Halfway through writing a transfer the
 * ledger holds one posting and is legitimately unbalanced; a trigger firing at
 * statement time would reject every transfer ever made. {@code DEFERRABLE
 * INITIALLY DEFERRED} moves the check to COMMIT, when the transaction is
 * complete and the invariant is supposed to hold. It is the database equivalent
 * of "check the invariant at the boundary, not in the middle".
 *
 * <p><b>Driven with raw JDBC and {@code autoCommit = false} on purpose.</b> Going
 * through {@code @Transactional} would surface the failure as a Spring
 * {@code TransactionSystemException} thrown from somewhere inside the proxy,
 * which obscures the one thing worth seeing: <b>the INSERT succeeded and the
 * COMMIT failed.</b> That is a genuinely unusual shape — most constraint
 * violations blow up at the statement — and it is what makes deferred
 * constraints worth talking about.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DeferredBalanceConstraintIT extends TestContainerConfig {

    /** SQLSTATE 23514 — check_violation, raised explicitly by assert_transfer_balanced(). */
    private static final String CHECK_VIOLATION = "23514";

    @Autowired
    private AccountRepository accounts;

    @Autowired
    private DataSource dataSource;

    @Test
    void aHalfWrittenTransferInsertsFineAndIsRejectedAtCommit() throws Exception {
        Account account = openAccount(accounts);

        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            long transferId = insertTransfer(c, account);

            // Only the debit. No matching credit anywhere.
            insertPosting(c, account.getId(), transferId, -500);

            assertThatThrownBy(c::commit)
                    .as("the statement was fine; the constraint runs at COMMIT and finds the transfer unbalanced")
                    .isInstanceOf(SQLException.class)
                    .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(CHECK_VIOLATION));
        }
    }

    @Test
    void abalancedPairCommitsCleanly() throws Exception {
        Account debit = openAccount(accounts);
        Account credit = openAccount(accounts);

        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            long transferId = insertTransfer(c, debit);

            insertPosting(c, debit.getId(), transferId, -500);
            insertPosting(c, credit.getId(), transferId, 500);

            c.commit();
        }
        // Reaching here without an exception is the assertion: the trigger fires
        // on both rows and sees a zero sum by the time it matters.
    }

    /**
     * The regression guard for phase 1. {@code phase1_isolation} posts standalone
     * debits with a null {@code transfer_id} — its whole write-skew demo depends
     * on it — so the trigger's first statement returns early for those. Without
     * that guard this test fails and takes {@code WriteSkewIT} with it.
     */
    @Test
    void aStandalonePostingWithNoTransferIsUnaffected() throws Exception {
        Account account = openAccount(accounts);

        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            insertPosting(c, account.getId(), null, -100);
            c.commit();
        }
    }

    private long insertTransfer(Connection c, Account from) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                """
                insert into transfers (idempotency_key, from_account_id, to_account_id, amount_minor)
                values (?, ?, ?, ?) returning id
                """)) {
            ps.setString(1, "deferred-it-" + UUID.randomUUID());
            ps.setLong(2, from.getId());
            ps.setLong(3, from.getId());
            ps.setLong(4, 500);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private void insertPosting(Connection c, long accountId, Long transferId, long amountMinor) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "insert into postings (account_id, transfer_id, amount_minor, note) values (?, ?, ?, 'deferred-it')")) {
            ps.setLong(1, accountId);
            if (transferId == null) {
                ps.setNull(2, java.sql.Types.BIGINT);
            } else {
                ps.setLong(2, transferId);
            }
            ps.setLong(3, amountMinor);
            ps.executeUpdate();
        }
    }
}

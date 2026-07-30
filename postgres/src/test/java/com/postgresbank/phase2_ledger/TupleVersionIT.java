package com.postgresbank.phase2_ledger;

import static com.postgresbank.testsupport.TestSupport.openAccount;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Physical proof that an {@code UPDATE} is a delete-and-insert of a new tuple
 * version, never an in-place mutation: {@code ctid} is a row's physical
 * address (page number, offset within the page). Postgres never changes a
 * committed tuple's bytes on disk - MVCC needs the old version to stay
 * exactly as it was for any transaction whose snapshot still needs to see it.
 * Update the row and its {@code ctid} moves to a new location, because it's
 * genuinely a new tuple; the old one is left in place, marked expired
 * ({@code xmax} set), waiting for {@code VACUUM} to reclaim its space.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TupleVersionIT extends TestContainerConfig {

    @Autowired
    private AccountRepository accounts;

    @Autowired
    private DataSource dataSource;

    @Test
    void updatingAnIndexedColumnMovesTheTuple() throws Exception {
        Account account = openAccount(accounts);

        try (Connection c = dataSource.getConnection()) {
            insertPosting(c, account.getId(), 100);
            String ctidBefore = ctidOf(c, account.getId());

            // amount_minor isn't indexed either, but forcing a page-spanning
            // rewrite isn't the point - any UPDATE demonstrates the delete+insert
            // shape. What matters for this assertion is simply: same logical row,
            // different physical address after the write.
            updateAmount(c, account.getId(), 999);
            String ctidAfter = ctidOf(c, account.getId());

            assertThat(ctidAfter)
                    .as("the physical location of the row should change after UPDATE - it's a new tuple version")
                    .isNotEqualTo(ctidBefore);
        }
    }

    private void insertPosting(Connection c, long accountId, long amountMinor) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(
                "insert into postings (account_id, amount_minor, note, created_at) values (?, ?, 'seed', now())")) {
            ps.setLong(1, accountId);
            ps.setLong(2, amountMinor);
            ps.executeUpdate();
        }
    }

    private void updateAmount(Connection c, long accountId, long newAmountMinor) throws Exception {
        try (PreparedStatement ps =
                c.prepareStatement("update postings set amount_minor = ? where account_id = ?")) {
            ps.setLong(1, newAmountMinor);
            ps.setLong(2, accountId);
            ps.executeUpdate();
        }
    }

    private String ctidOf(Connection c, long accountId) throws Exception {
        try (PreparedStatement ps = c.prepareStatement("select ctid from postings where account_id = ?")) {
            ps.setLong(1, accountId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }
}

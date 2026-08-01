package com.postgresbank.phase5_indexing;

import static com.postgresbank.testsupport.ExplainSupport.explain;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.testsupport.ExplainSupport.QueryPlan;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * A <b>partial index</b> indexes only the rows matching its own {@code WHERE}
 * clause. {@code idx_outbox_unpublished} covers {@code (id) WHERE NOT published},
 * which is the entire query the relay ever runs.
 *
 * <p><b>Why this shape fits an outbox so exactly.</b> The table grows forever —
 * it is a log — but the interesting set is only ever the handful of rows not yet
 * relayed. A full index on {@code published} would grow with history and be
 * almost entirely made of rows nobody will ever ask about; the partial index
 * stays proportional to the <em>backlog</em>. Same story for the
 * {@code payment_jobs} queue, and for any "pending / unprocessed / not yet sent"
 * flag, which is most of them.
 *
 * <p>The requirement to remember: the planner can only use a partial index when
 * it can prove the query's predicate implies the index's. {@code WHERE NOT
 * published} matches; {@code WHERE published = false} also matches; a query with
 * no predicate on {@code published} at all does not, and silently gets a
 * sequential scan.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PartialIndexIT extends TestContainerConfig {

    private static final int PUBLISHED_ROWS = 50_000;

    @Autowired
    private DataSource dataSource;

    /**
     * Seeds bulk history as <b>published</b> only. Unpublished rows would be
     * swept up by the scheduled {@code OutboxRelay} within two seconds anyway,
     * and — more importantly — leaving a backlog behind would change what other
     * ITs in this module see. The plan choice is what is under test, not the row
     * count.
     */
    @BeforeEach
    void seedPublishedHistory() throws Exception {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(true);
            if (countPublished(c) >= PUBLISHED_ROWS) {
                return;
            }
            try (PreparedStatement ps = c.prepareStatement(
                    """
                    insert into outbox (event_id, payload, published)
                    select gen_random_uuid(), 'partial-index-fixture-' || g, true
                    from generate_series(1, ?) g
                    """)) {
                ps.setInt(1, PUBLISHED_ROWS);
                ps.executeUpdate();
            }
            try (Statement st = c.createStatement()) {
                st.execute("analyze outbox");
            }
        }
    }

    @Test
    void theRelaysQueryIsServedByThePartialIndex() throws Exception {
        QueryPlan plan = explain(dataSource, "select id from outbox where not published order by id limit 100");

        assertThat(plan.indexNames())
                .as("the predicate matches the index's own WHERE clause, so it is usable. Plan: %s", plan.nodeTypes())
                .contains("idx_outbox_unpublished");
    }

    /**
     * The other half, and the part that catches people out: the same table, a
     * query that does not mention {@code published}, and the partial index is
     * simply unavailable — it does not contain the rows being asked for.
     */
    @Test
    void aQueryWithoutThePredicateCannotUseThePartialIndex() throws Exception {
        QueryPlan plan = explain(dataSource, "select count(*) from outbox");

        assertThat(plan.indexNames())
                .as("no predicate on published means the partial index cannot answer this. Plan: %s", plan.nodeTypes())
                .doesNotContain("idx_outbox_unpublished");
    }

    private static long countPublished(Connection c) throws Exception {
        try (Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("select count(*) from outbox where published")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}

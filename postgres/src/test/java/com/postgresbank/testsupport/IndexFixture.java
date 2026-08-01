package com.postgresbank.testsupport;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;

/**
 * A table big enough for the planner to have an opinion.
 *
 * <p>Index tests are meaningless on ten rows: Postgres will seq-scan a small
 * table regardless of what indexes exist, and it is right to. These seed ~100k
 * postings so the choice between an index scan and a sequential scan is a real
 * one that the planner makes on cost.
 *
 * <p><b>Two accounts, deliberately lopsided.</b> One holds a couple of hundred
 * rows and one holds essentially all of them. That gives both plan shapes on the
 * same table with no DDL at all: the selective account gets an Index Scan, the
 * bulk account correctly gets a Seq Scan, because reading 99% of a table through
 * an index is slower than just reading the table. The alternative — dropping the
 * index to force a Seq Scan — mutates state every other IT depends on, and
 * teaches less. <b>A sequential scan is sometimes the right answer</b> is the
 * lesson worth having.
 *
 * <p><b>These two accounts break the suite's usual "unique data per test" rule
 * on purpose.</b> Every other test opens {@code "owner-" + UUID.randomUUID()}
 * because the container is reused and never reset. These are found-or-created by
 * fixed name, seeded once per container lifetime, and never mutated by anything —
 * they are read-only fixtures, and re-seeding 100k rows for each test class would
 * dominate the run time for no benefit.
 *
 * <p>Seeded with one {@code INSERT ... SELECT FROM generate_series}, which takes
 * well under a second. Doing it through {@code postings.saveAll()} of 100k
 * entities would take minutes and prove nothing about indexes.
 */
public final class IndexFixture {

    public static final int BULK_ROWS = 100_000;
    public static final int SELECTIVE_ROWS = 200;

    private static final String SELECTIVE_OWNER = "index-fixture-selective";
    private static final String BULK_OWNER = "index-fixture-bulk";

    private IndexFixture() {}

    /** The two account ids, and the guarantee that the rows behind them exist. */
    public record Fixture(long selectiveAccountId, long bulkAccountId) {}

    public static Fixture seedOnce(DataSource dataSource) throws Exception {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(true);
            long selective = findOrCreateAccount(c, SELECTIVE_OWNER);
            long bulk = findOrCreateAccount(c, BULK_OWNER);

            if (countPostings(c, bulk) >= BULK_ROWS) {
                return new Fixture(selective, bulk);
            }

            insertPostings(c, bulk, BULK_ROWS);
            insertPostings(c, selective, SELECTIVE_ROWS);

            // Without this the planner is working from stale statistics - it has no
            // idea 100k rows just arrived - and will happily seq-scan the selective
            // query, making the whole fixture pointless. Stale stats are also the
            // real-world answer to "why did the plan suddenly get worse?", so this
            // line is the lesson as much as it is setup.
            analyze(c);
            return new Fixture(selective, bulk);
        }
    }

    private static long findOrCreateAccount(Connection c, String owner) throws Exception {
        try (PreparedStatement find = c.prepareStatement("select id from accounts where owner = ?")) {
            find.setString(1, owner);
            try (ResultSet rs = find.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        try (PreparedStatement insert =
                c.prepareStatement("insert into accounts (owner) values (?) returning id")) {
            insert.setString(1, owner);
            try (ResultSet rs = insert.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static long countPostings(Connection c, long accountId) throws Exception {
        try (PreparedStatement ps = c.prepareStatement("select count(*) from postings where account_id = ?")) {
            ps.setLong(1, accountId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    /**
     * Distinct, descending {@code created_at} per row, so the ordering tests have
     * something real to order by and keyset pagination has a total order to seek
     * within. amount_minor is 1 rather than 0 because of the
     * {@code postings_amount_nonzero} CHECK.
     */
    private static void insertPostings(Connection c, long accountId, int rows) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(
                """
                insert into postings (account_id, transfer_id, amount_minor, note, created_at)
                select ?, null, 1, 'index-fixture', now() - (g || ' seconds')::interval
                from generate_series(1, ?) g
                """)) {
            ps.setLong(1, accountId);
            ps.setInt(2, rows);
            ps.executeUpdate();
        }
    }

    private static void analyze(Connection c) throws Exception {
        try (Statement st = c.createStatement()) {
            // ANALYZE, unlike VACUUM, is allowed inside a transaction block - but
            // this connection is on autocommit anyway.
            st.execute("analyze postings");
        }
    }
}

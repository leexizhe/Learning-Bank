package com.postgresbank.testsupport;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;

/**
 * A table big enough for the planner to have an opinion.
 *
 * <p>Index tests are meaningless on ten rows: Postgres will seq-scan a small table regardless of what indexes exist,
 * and it is right to. These seed ~100k postings so the choice between an index scan and a sequential scan is a real one
 * that the planner makes on cost.
 *
 * <p><b>Two accounts, deliberately lopsided.</b> One holds a couple of thousand rows and one holds essentially all of
 * them. That gives both plan shapes on the same table with no DDL at all: the selective account gets an Index Scan, the
 * bulk account correctly gets a Seq Scan, because reading 99% of a table through an index is slower than just reading
 * the table. The alternative — dropping the index to force a Seq Scan — mutates state every other IT depends on, and
 * teaches less. <b>A sequential scan is sometimes the right answer</b> is the lesson worth having.
 *
 * <p><b>These two accounts break the suite's usual "unique data per test" rule on purpose.</b> Every other test opens
 * {@code "owner-" + UUID.randomUUID()} because the container is reused and never reset. These are found-or-created by
 * fixed name, seeded once per container lifetime, and never mutated by anything — they are read-only fixtures, and
 * re-seeding 100k rows for each test class would dominate the run time for no benefit.
 *
 * <p>Seeded with one {@code INSERT ... SELECT FROM generate_series}, which takes well under a second. Doing it through
 * {@code postings.saveAll()} of 100k entities would take minutes and prove nothing about indexes.
 */
public final class IndexFixture {

    public static final int BULK_ROWS = 100_000;

    /**
     * Big enough that walking the composite index in order beats sorting the whole account.
     *
     * <p>This number is load-bearing for {@code Phase5IndexingIT.CompositeOrderTests}, and 200 was on the wrong side
     * of the line. At 200 rows Postgres correctly reads the narrower {@code idx_postings_account_id} and top-N
     * heapsorts the result — cost 28.9 against 32.8 for the ordered walk — so the "right index removes the Sort"
     * assertion failed on a database with accurate statistics. The plan flips between 250 and 300 rows; 2000 clears it
     * by a wide margin (cost 20.2 against ~1900 to sort) while staying ~2% of the table, so the selective-vs-bulk
     * contrast in {@code Phase5IndexingIT.IndexPlanTests} still holds.
     */
    public static final int SELECTIVE_ROWS = 2_000;

    private static final String SELECTIVE_OWNER = "index-fixture-selective";
    private static final String BULK_OWNER = "index-fixture-bulk";

    /**
     * Once per JVM, as the name promises. Without this, "seed once" meant "re-check on every call" — a connection plus
     * four queries, one of them a {@code count(*)} over all 100k rows, before every single test that touches the
     * fixture. The rows are documented read-only and nothing in the suite deletes them, so the second call has nothing
     * left to learn.
     */
    private static volatile Fixture cached;

    private IndexFixture() {}

    /** The two account ids, and the guarantee that the rows behind them exist. */
    public record Fixture(long selectiveAccountId, long bulkAccountId) {}

    public static Fixture seedOnce(DataSource dataSource) throws Exception {
        Fixture existing = cached;
        if (existing != null) {
            return existing;
        }
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(true);
            long selective = findOrCreateAccount(c, SELECTIVE_OWNER);
            long bulk = findOrCreateAccount(c, BULK_OWNER);

            // Each account is checked and seeded independently, and to an exact count. The two inserts are separate
            // autocommitted statements, so a run that dies between them leaves bulk seeded and selective empty; a
            // guard that only looked at bulk would accept that state forever, handing every later test an account
            // with no rows and a plan to match. Phase5IndexingIT.IndexPlanTests asserts the row count exactly, so
            // "top up whatever is missing" is not good enough either - a wrong count is reset rather than added to.
            boolean seeded = reseedIfWrongSize(c, bulk, BULK_ROWS);
            seeded |= reseedIfWrongSize(c, selective, SELECTIVE_ROWS);

            if (seeded) {
                // Without this the planner is working from stale statistics - it has no idea 100k rows just arrived -
                // and will happily seq-scan the selective query, making the whole fixture pointless. Stale stats are
                // also the real-world answer to "why did the plan suddenly get worse?", so this line is the lesson as
                // much as it is setup.
                analyze(c);
            }
            Fixture fixture = new Fixture(selective, bulk);
            cached = fixture;
            return fixture;
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
        try (PreparedStatement insert = c.prepareStatement("insert into accounts (owner) values (?) returning id")) {
            insert.setString(1, owner);
            try (ResultSet rs = insert.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    /** Returns true if it had to write anything, so the caller knows whether the statistics need refreshing. */
    private static boolean reseedIfWrongSize(Connection c, long accountId, int rows) throws Exception {
        if (countPostings(c, accountId) == rows) {
            return false;
        }
        deletePostings(c, accountId);
        insertPostings(c, accountId, rows);
        return true;
    }

    private static void deletePostings(Connection c, long accountId) throws Exception {
        try (PreparedStatement ps = c.prepareStatement("delete from postings where account_id = ?")) {
            ps.setLong(1, accountId);
            ps.executeUpdate();
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
     * Distinct, descending {@code created_at} per row, so the ordering tests have something real to order by and keyset
     * pagination has a total order to seek within. amount_minor is 1 rather than 0 because of the {@code
     * postings_amount_nonzero} CHECK.
     */
    private static void insertPostings(Connection c, long accountId, int rows) throws Exception {
        try (PreparedStatement ps = c.prepareStatement("""
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
            // ANALYZE, unlike VACUUM, is allowed inside a transaction block - but this connection is on autocommit
            // anyway.
            st.execute("analyze postings");
        }
    }
}

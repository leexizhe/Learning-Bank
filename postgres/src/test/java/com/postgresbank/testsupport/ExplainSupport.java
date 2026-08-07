package com.postgresbank.testsupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/**
 * Runs {@code EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)} and exposes the plan as something assertable.
 *
 * <p>A sibling of {@link TestSupport} rather than more methods on it: this needs a nested plan type and a recursive
 * tree walk, which is a different concern from "open an account, read a stat counter".
 *
 * <p><b>Why JSON and not string matching.</b> Grepping {@code EXPLAIN} text for {@code "Index Scan"} matches {@code
 * "Index Only Scan"} and {@code "Bitmap Index Scan"} too, and it cannot tell a top-level {@code Seq Scan} from one
 * buried in a subplan — so the assertion silently means something other than what it says. Parsing the JSON gives the
 * actual tree. Jackson needs no new dependency; it is already on the classpath via {@code spring-boot-starter-web}.
 *
 * <p><b>Why {@code ANALYZE}.</b> Without it the numbers are the planner's estimates. With it the query really runs and
 * the plan carries actual row counts and real buffer reads — which is what makes the estimated-vs-actual gap visible,
 * the single most useful thing in a plan when something is slow. A 1000x discrepancy means the statistics are stale and
 * the fix is {@code ANALYZE}, not a new index.
 *
 * <p>Parameters go through a {@link PreparedStatement}. The JDBC driver's {@code prepareThreshold} defaults to 5, so
 * the first few executions are planned with the parameter values substituted (a <em>custom</em> plan) rather than the
 * one-size-fits-all generic plan — which matters here, because the whole point of these tests is that the planner
 * chooses differently for a selective value than for one matching most of the table.
 */
public final class ExplainSupport {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ExplainSupport() {}

    public static QueryPlan explain(DataSource dataSource, String sql, Object... params) throws Exception {
        try (Connection c = dataSource.getConnection()) {
            return explain(c, sql, params);
        }
    }

    public static QueryPlan explain(Connection c, String sql, Object... params) throws Exception {
        try (PreparedStatement ps = c.prepareStatement("explain (analyze, buffers, format json) " + sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                JsonNode document = MAPPER.readTree(rs.getString(1));
                return new QueryPlan(document.get(0).get("Plan"));
            }
        }
    }

    /** One parsed plan tree, rooted at the outermost node. */
    public record QueryPlan(JsonNode root) {

        /** e.g. {@code "Seq Scan"}, {@code "Index Scan"}, {@code "Sort"}, {@code "Limit"}. */
        public String rootNodeType() {
            return root.get("Node Type").asText();
        }

        /** Every node type in the tree, outermost first. */
        public List<String> nodeTypes() {
            List<String> types = new ArrayList<>();
            collect(root, node -> types.add(node.get("Node Type").asText()));
            return types;
        }

        /**
         * Index names actually used, which is how you assert that the <em>intended</em> index was chosen.
         */
        public List<String> indexNames() {
            List<String> names = new ArrayList<>();
            collect(root, node -> {
                JsonNode name = node.get("Index Name");
                if (name != null) {
                    names.add(name.asText());
                }
            });
            return names;
        }

        /** True for Index Scan, Index Only Scan or Bitmap Index Scan anywhere in the tree. */
        public boolean usesIndex() {
            return nodeTypes().stream().anyMatch(type -> type.contains("Index"));
        }

        /**
         * A sort node means the index did not deliver rows in the order the query asked for, so Postgres had to order
         * them itself. Its absence is the sharpest available evidence that a composite index's column order is right.
         *
         * <p>Matches on <em>contains</em> rather than equality, because there is more than one sort node type. {@code
         * "Incremental Sort"} (Postgres 13+) appears when the input is already sorted by a <em>prefix</em> of the
         * required keys: Postgres sorts within each group of equal prefix values instead of sorting everything, so it
         * can start returning rows before consuming the whole input. Cheaper than a full {@code Sort} — but still a
         * sort, and still evidence that the index did not fully satisfy the {@code ORDER BY}. An exact-equality check
         * here silently passes on exactly the case it is meant to catch.
         */
        public boolean hasSortNode() {
            return nodeTypes().stream().anyMatch(type -> type.contains("Sort"));
        }

        /**
         * Total 8KB buffers touched. Read off the root node rather than summed over the tree: Postgres reports these
         * cumulatively, so a child's blocks are already included in its parent's and adding them up double-counts.
         *
         * <p>This is the honest way to compare two query shapes. Wall-clock time measures the machine's mood; buffers
         * measure the work the query actually asked the storage layer to do.
         */
        public long sharedBlocks() {
            return longAt(root, "Shared Hit Blocks") + longAt(root, "Shared Read Blocks");
        }

        /** Rows the query really returned, as opposed to what the planner guessed. */
        public long actualRows() {
            return longAt(root, "Actual Rows");
        }

        private static long longAt(JsonNode node, String field) {
            JsonNode value = node.get(field);
            return value == null ? 0L : value.asLong();
        }

        private static void collect(JsonNode node, java.util.function.Consumer<JsonNode> visitor) {
            visitor.accept(node);
            JsonNode children = node.get("Plans");
            if (children != null) {
                children.forEach(child -> collect(child, visitor));
            }
        }
    }
}

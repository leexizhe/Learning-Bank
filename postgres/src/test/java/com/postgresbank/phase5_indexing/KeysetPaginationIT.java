package com.postgresbank.phase5_indexing;

import static com.postgresbank.testsupport.ExplainSupport.explain;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Posting;
import com.postgresbank.common.PostingRepository;
import com.postgresbank.testsupport.ExplainSupport.QueryPlan;
import com.postgresbank.testsupport.IndexFixture;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

/**
 * {@code LIMIT/OFFSET} versus keyset pagination, compared on <b>buffers read</b> rather than on
 * wall-clock time — the same principle as the rest of this module: measure the work the query asked
 * the storage layer to do, not how busy the machine happened to be.
 *
 * <p><b>Why OFFSET degrades.</b> There is no way to skip rows without producing them. {@code OFFSET
 * 50000} reads fifty thousand rows through the index, throws every one away, and returns the next
 * twenty. The cost is O(offset + size), so page 1 is instant, page 2500 is slow, and the endpoint
 * gets steadily worse in a way that never shows up in testing with 10 rows of data.
 *
 * <p>Keyset instead says "everything strictly after this row", which the B-tree can seek to
 * directly: O(page size) at any depth. The catch is real and worth stating — no page numbers, no
 * total count, no jumping to an arbitrary page — which is why both endpoints exist rather than one
 * replacing the other.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class KeysetPaginationIT extends TestContainerConfig {

  private static final int PAGE_SIZE = 20;
  private static final int DEEP_OFFSET = 50_000;

  @Autowired private DataSource dataSource;

  @Autowired private PostingRepository postings;

  private IndexFixture.Fixture fixture;

  @BeforeEach
  void seed() throws Exception {
    fixture = IndexFixture.seedOnce(dataSource);
  }

  @Test
  void aDeepOffsetPageReadsFarMoreOfTheIndexThanTheEquivalentKeysetPage() throws Exception {
    long accountId = fixture.bulkAccountId();
    Cursor cursor = cursorAtOffset(accountId, DEEP_OFFSET);

    QueryPlan offsetPlan =
        explain(
            dataSource,
            "select id, created_at from postings where account_id = ? order by created_at desc, id desc limit ? offset ?",
            accountId,
            PAGE_SIZE,
            DEEP_OFFSET);

    QueryPlan keysetPlan =
        explain(
            dataSource,
            "select id, created_at from postings where account_id = ? and (created_at, id) < (?, ?) order by created_at desc, id desc limit ?",
            accountId,
            Timestamp.from(cursor.createdAt()),
            cursor.id(),
            PAGE_SIZE);

    assertThat(keysetPlan.sharedBlocks())
        .as(
            "keyset seeks straight to the row; offset reads and discards %d of them first (offset=%d blocks, keyset=%d blocks)",
            DEEP_OFFSET, offsetPlan.sharedBlocks(), keysetPlan.sharedBlocks())
        .isLessThan(offsetPlan.sharedBlocks());

    assertThat(keysetPlan.hasSortNode())
        .as("the composite index already provides the order, so neither plan needs a Sort")
        .isFalse();
  }

  /**
   * Both sides go through Hibernate on purpose. Reading the cursor with raw JDBC ({@code
   * ResultSet.getTimestamp().toInstant()}) and then feeding that {@code Instant} back through a
   * Hibernate-bound parameter compares two different interpretations of a {@code TIMESTAMP WITHOUT
   * TIME ZONE} — the driver resolves it against the JVM default zone, Hibernate against its own —
   * and the seek silently lands hours away from where the offset page is. Which is itself the
   * argument for {@code TIMESTAMPTZ} over {@code TIMESTAMP} for anything a cursor is built from.
   */
  @Test
  void bothApproachesReturnTheSamePage() {
    long accountId = fixture.bulkAccountId();
    Sort newestFirst = Sort.by(Sort.Order.desc("createdAt"), Sort.Order.desc("id"));

    // The last row of the preceding page is exactly the cursor a client holds.
    List<Posting> precedingPage =
        postings
            .findByAccountId(
                accountId, PageRequest.of(DEEP_OFFSET / PAGE_SIZE - 1, PAGE_SIZE, newestFirst))
            .getContent();
    Posting cursor = precedingPage.get(precedingPage.size() - 1);

    List<Long> viaOffset =
        postings
            .findByAccountId(
                accountId, PageRequest.of(DEEP_OFFSET / PAGE_SIZE, PAGE_SIZE, newestFirst))
            .getContent()
            .stream()
            .map(Posting::getId)
            .toList();

    List<Long> viaKeyset =
        postings
            .seekByAccountId(accountId, cursor.getCreatedAt(), cursor.getId(), PAGE_SIZE)
            .stream()
            .map(Posting::getId)
            .toList();

    assertThat(viaKeyset)
        .as(
            "same rows, same order - keyset is a cheaper route to an identical answer, not a different one")
        .isEqualTo(viaOffset);
    assertThat(viaKeyset).hasSize(PAGE_SIZE);
  }

  /**
   * The (created_at, id) of the last row before the page at {@code offset} — i.e. the cursor a
   * client would hold.
   */
  private Cursor cursorAtOffset(long accountId, int offset) throws Exception {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps =
            c.prepareStatement(
                "select created_at, id from postings where account_id = ? order by created_at desc, id desc limit 1 offset ?")) {
      ps.setLong(1, accountId);
      ps.setInt(2, offset - 1);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return new Cursor(rs.getTimestamp(1).toInstant(), rs.getLong(2));
      }
    }
  }

  private record Cursor(Instant createdAt, long id) {}
}

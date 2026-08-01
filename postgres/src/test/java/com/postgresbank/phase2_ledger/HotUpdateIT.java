package com.postgresbank.phase2_ledger;

import static com.postgresbank.testsupport.TestSupport.openAccount;
import static com.postgresbank.testsupport.TestSupport.readStatCounter;
import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import com.postgresbank.common.Posting;
import com.postgresbank.common.PostingRepository;
import java.time.Duration;
import javax.sql.DataSource;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * {@code note} on {@code postings} is not indexed (see schema.sql / {@code Posting}). Updating only
 * that column lets Postgres do a HOT (Heap-Only Tuple) update: the new tuple version can be written
 * to the same page without touching any index, because no indexed column changed and there's room
 * on the page. {@code pg_stat_user_tables.n_tup_hot_upd} counts exactly these updates - this test
 * proves the count moves, rather than just asserting the update "worked" (which a plain non-HOT
 * update would also do).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HotUpdateIT extends TestContainerConfig {

  @Autowired private AccountRepository accounts;

  @Autowired private PostingRepository postings;

  @Autowired private DataSource dataSource;

  @Test
  void updatingANonIndexedColumnRegistersAsHotUpdates() throws Exception {
    Account account = openAccount(accounts);
    Posting posting = postings.save(new Posting(account, null, 500, "initial-note"));

    long hotUpdatesBefore = readStatCounter(dataSource, "postings", "n_tup_hot_upd");

    for (int i = 0; i < 20; i++) {
      posting.setNote("note-revision-" + i);
      postings.save(posting);
      postings.flush();
    }

    // pg_stat_user_tables is refreshed from shared memory on commit but a
    // fresh connection's first read can lag it by a beat - poll rather than
    // asserting on a single snapshot.
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                assertThat(readStatCounter(dataSource, "postings", "n_tup_hot_upd"))
                    .as(
                        "updating only the non-indexed 'note' column should register as HOT updates")
                    .isGreaterThan(hotUpdatesBefore));
  }
}

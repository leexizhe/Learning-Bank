package com.postgresbank.common;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface AccountRepository extends JpaRepository<Account, Long> {

  /**
   * The N+1 fix for phase4_performance: one query, postings joined in via the same round trip.
   * Compare with the default {@code findAllById}, where touching {@code account.getPostings()}
   * afterward triggers one extra SELECT per account.
   */
  @Query("select distinct a from Account a left join fetch a.postings where a.id in :ids")
  List<Account> findAllFetchPostingsByIdIn(@Param("ids") List<Long> ids);
}

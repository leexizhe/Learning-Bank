package com.postgresbank.common;

import java.util.Collection;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PostingRepository extends JpaRepository<Posting, Long> {

    /** Balance-as-projection: the only place a balance is ever computed. */
    @Query("select coalesce(sum(p.amountMinor), 0) from Posting p where p.account.id = :accountId")
    long sumAmountByAccountId(@Param("accountId") long accountId);

    /** Same projection, summed across several accounts in one round trip - see {@code LedgerService.combinedBalanceOf}. */
    @Query("select coalesce(sum(p.amountMinor), 0) from Posting p where p.account.id in :accountIds")
    long sumAmountByAccountIdIn(@Param("accountIds") Collection<Long> accountIds);

    Page<Posting> findByAccountId(long accountId, Pageable pageable);

    long countByTransferId(Long transferId);
}

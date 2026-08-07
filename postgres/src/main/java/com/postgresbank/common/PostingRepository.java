package com.postgresbank.common;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PostingRepository extends JpaRepository<Posting, Long> {

    /** Balance-as-projection: the only place a balance is ever computed. */
    @Query("select coalesce(sum(p.amountMinor), 0) from Posting p where p.account.id = :accountId")
    long sumAmountByAccountId(@Param("accountId") long accountId);

    /**
     * Same projection, summed across several accounts in one round trip - see {@code LedgerService.combinedBalanceOf}.
     */
    @Query("select coalesce(sum(p.amountMinor), 0) from Posting p where p.account.id in :accountIds")
    long sumAmountByAccountIdIn(@Param("accountIds") Collection<Long> accountIds);

    /** The delta half of a snapshot read: only the postings written since the snapshot was taken. */
    @Query("select coalesce(sum(p.amountMinor), 0) from Posting p where p.account.id = :accountId and p.id > :afterId")
    long sumAmountByAccountIdAfterId(@Param("accountId") long accountId, @Param("afterId") long afterId);

    /**
     * The balance half of a snapshot: everything up to and including the recorded high-water mark.
     */
    @Query("select coalesce(sum(p.amountMinor), 0) from Posting p where p.account.id = :accountId and p.id <= :upToId")
    long sumAmountByAccountIdUpToId(@Param("accountId") long accountId, @Param("upToId") long upToId);

    /** The high-water mark a snapshot is taken "as of". Null when the account has no postings yet. */
    @Query("select max(p.id) from Posting p where p.account.id = :accountId")
    Long maxIdByAccountId(@Param("accountId") long accountId);

    Page<Posting> findByAccountId(long accountId, Pageable pageable);

    /**
     * Keyset (or "seek") pagination: instead of counting past N rows, jump straight to the last row the caller saw and
     * read forward from there.
     *
     * <p>The predicate is a <b>row comparison</b>, {@code (created_at, id) < (:ts, :id)}, not {@code created_at < :ts
     * OR (created_at = :ts AND id < :id)}. The two are logically identical and only the first is a single range
     * condition the B-tree can seek on directly — the OR form makes the planner choose between two branches and usually
     * costs a bitmap or a sort. Written natively because JPQL has no row-constructor comparison.
     *
     * <p>{@code id} is in the key so the order is <b>total</b>. With {@code created_at} alone, rows sharing a timestamp
     * have no defined order between pages, so one can be shown twice and another skipped — the bug that makes offset
     * pagination look "mostly fine" until it silently isn't.
     *
     * <p>Cost is O(page size) at any depth, against OFFSET's O(offset + size). The trade: no page numbers and no "jump
     * to page 500", because there is no count. Feeds and statements can live with that; a table with a page picker
     * cannot, which is why both endpoints exist here rather than one replacing the other.
     */
    @Query(value = """
                    select * from postings
                    where account_id = :accountId and (created_at, id) < (:afterCreatedAt, :afterId)
                    order by created_at desc, id desc
                    limit :size
                    """, nativeQuery = true)
    List<Posting> seekByAccountId(
            @Param("accountId") long accountId,
            @Param("afterCreatedAt") Instant afterCreatedAt,
            @Param("afterId") long afterId,
            @Param("size") int size);

    long countByTransferId(Long transferId);
}

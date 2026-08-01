package com.postgresbank.phase4_performance;

import com.postgresbank.common.PostingRepository;
import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * A real transaction-history endpoint has to paginate - loading every posting an account has ever
 * made into memory to render one page doesn't scale, and it's the kind of thing that works fine in
 * a demo with 10 rows and falls over in production with 10 million. {@code Pageable} here comes
 * straight off the query string ({@code ?page=0&size=20&sort=id,desc}) via Spring Data's built-in
 * resolver - no manual LIMIT/OFFSET arithmetic.
 *
 * <p>Returns a hand-built response record rather than the {@code Page} returned by the repository,
 * for two reasons: first, {@code Posting} carries a LAZY {@code account} reference that Jackson
 * can't touch once {@code open-in-view: false} has closed the session; second, Spring Data's own
 * JSON shape for {@code Page} has changed across versions (flat fields vs. a nested {@code page}
 * object) - owning the shape here keeps the API contract stable regardless of that.
 */
@RestController
public class PostingHistoryController {

  private final PostingRepository postings;

  public PostingHistoryController(PostingRepository postings) {
    this.postings = postings;
  }

  @GetMapping("/api/accounts/{accountId}/postings")
  public PagedPostings history(@PathVariable long accountId, Pageable pageable) {
    Page<PostingView> page =
        postings
            .findByAccountId(accountId, pageable)
            .map(
                p -> new PostingView(p.getId(), p.getAmountMinor(), p.getNote(), p.getCreatedAt()));
    return new PagedPostings(
        page.getContent(),
        page.getTotalElements(),
        page.getTotalPages(),
        page.getNumber(),
        page.getSize());
  }

  /**
   * The same history, paginated by <b>seeking</b> rather than counting. The caller passes back the
   * {@code created_at} and {@code id} of the last row it saw and gets the next page, in O(page
   * size) work regardless of how deep it has scrolled — whereas {@code LIMIT/OFFSET} reads and
   * discards every row before the offset, so page 5000 does 5000 pages of work to return 20 rows.
   *
   * <p>Omit the cursor for the first page. The response carries the cursor for the next one, and
   * nulls when there are no more rows — no page numbers and no total, which is the honest cost of
   * the approach: there is no COUNT, so there is no "jump to page 500". Feeds and statements can
   * live with that; a grid with a page picker cannot, which is why {@code /postings} still exists
   * alongside this.
   *
   * <p>{@code KeysetPaginationIT} compares the two on <b>buffers read</b> rather than wall-clock
   * time.
   */
  @GetMapping("/api/accounts/{accountId}/postings/seek")
  public SeekPage seek(
      @PathVariable long accountId,
      @RequestParam(required = false) Instant afterCreatedAt,
      @RequestParam(required = false) Long afterId,
      @RequestParam(defaultValue = "20") int size) {

    // No cursor means "start at the top": a timestamp far enough in the future
    // that every row sorts below it, which keeps one query serving both cases
    // rather than branching on a null.
    Instant cursorTime =
        afterCreatedAt != null ? afterCreatedAt : Instant.now().plusSeconds(86_400);
    long cursorId = afterId != null ? afterId : Long.MAX_VALUE;

    List<PostingView> rows =
        postings.seekByAccountId(accountId, cursorTime, cursorId, size).stream()
            .map(p -> new PostingView(p.getId(), p.getAmountMinor(), p.getNote(), p.getCreatedAt()))
            .toList();

    PostingView last = rows.isEmpty() ? null : rows.get(rows.size() - 1);
    return new SeekPage(
        rows, last == null ? null : last.createdAt(), last == null ? null : last.id());
  }

  public record PostingView(Long id, long amountMinor, String note, Instant createdAt) {}

  /**
   * {@code nextCreatedAt}/{@code nextId} are the cursor for the following page, or null at the end.
   */
  public record SeekPage(List<PostingView> content, Instant nextCreatedAt, Long nextId) {}

  public record PagedPostings(
      List<PostingView> content, long totalElements, int totalPages, int page, int size) {}
}

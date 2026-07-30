package com.postgresbank.phase4_performance;

import com.postgresbank.common.PostingRepository;
import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * A real transaction-history endpoint has to paginate - loading every
 * posting an account has ever made into memory to render one page doesn't
 * scale, and it's the kind of thing that works fine in a demo with 10 rows
 * and falls over in production with 10 million. {@code Pageable} here comes
 * straight off the query string ({@code ?page=0&size=20&sort=id,desc}) via
 * Spring Data's built-in resolver - no manual LIMIT/OFFSET arithmetic.
 *
 * <p>Returns a hand-built response record rather than the {@code Page}
 * returned by the repository, for two reasons: first, {@code Posting} carries
 * a LAZY {@code account} reference that Jackson can't touch once
 * {@code open-in-view: false} has closed the session; second, Spring Data's
 * own JSON shape for {@code Page} has changed across versions (flat fields
 * vs. a nested {@code page} object) - owning the shape here keeps the API
 * contract stable regardless of that.
 */
@RestController
public class PostingHistoryController {

    private final PostingRepository postings;

    public PostingHistoryController(PostingRepository postings) {
        this.postings = postings;
    }

    @GetMapping("/api/accounts/{accountId}/postings")
    public PagedPostings history(@PathVariable long accountId, Pageable pageable) {
        Page<PostingView> page = postings.findByAccountId(accountId, pageable)
                .map(p -> new PostingView(p.getId(), p.getAmountMinor(), p.getNote(), p.getCreatedAt()));
        return new PagedPostings(
                page.getContent(), page.getTotalElements(), page.getTotalPages(), page.getNumber(), page.getSize());
    }

    public record PostingView(Long id, long amountMinor, String note, Instant createdAt) {}

    public record PagedPostings(
            List<PostingView> content, long totalElements, int totalPages, int page, int size) {}
}

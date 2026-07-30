package com.postgresbank.phase4_performance;

import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Two ways to load "these accounts, with their posting history" - same
 * result, very different SQL. {@link #loadNPlusOne} is the mistake: one
 * query for the accounts, then Hibernate silently issues one more query
 * <em>per account</em> the moment {@code getPostings()} is touched, because
 * the association is LAZY (see {@link Account}). {@link #loadFetchJoined}
 * is the fix - a single query with {@code JOIN FETCH} pulls accounts and
 * their postings together. NPlusOneIT proves the query counts directly via
 * Hibernate's {@code Statistics}, rather than just trusting that JOIN FETCH
 * "should" be faster.
 */
@Service
public class AccountHistoryService {

    private final AccountRepository accounts;

    public AccountHistoryService(AccountRepository accounts) {
        this.accounts = accounts;
    }

    @Transactional(readOnly = true)
    public int loadNPlusOne(List<Long> accountIds) {
        List<Account> loaded = accounts.findAllById(accountIds);
        int totalPostings = 0;
        for (Account account : loaded) {
            // Each .size() call here is where the extra SELECT fires - the
            // association wasn't fetched with the accounts query above.
            totalPostings += account.getPostings().size();
        }
        return totalPostings;
    }

    @Transactional(readOnly = true)
    public int loadFetchJoined(List<Long> accountIds) {
        return accounts.findAllFetchPostingsByIdIn(accountIds).stream()
                .mapToInt(a -> a.getPostings().size())
                .sum();
    }
}

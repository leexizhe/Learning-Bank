package com.postgresbank.common;

import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class AccountController {

    private final AccountRepository accounts;
    private final LedgerService ledger;

    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping("/api/accounts")
    public AccountView open(@RequestBody CreateAccountRequest request) {
        Account saved = accounts.save(new Account(request.owner()));
        return new AccountView(saved.getId(), saved.getOwner(), 0);
    }

    @GetMapping("/api/accounts/{id}")
    public AccountView view(@PathVariable long id) {
        Account account = accounts.findById(id).orElseThrow();
        return new AccountView(account.getId(), account.getOwner(), ledger.balanceOf(id));
    }

    public record CreateAccountRequest(@NotBlank String owner) {}

    public record AccountView(Long id, String owner, long balanceMinor) {}
}

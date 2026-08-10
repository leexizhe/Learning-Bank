package com.concurrencybank.phase4_ledger.controller;

import com.concurrencybank.phase4_ledger.dto.AccountResponse;
import com.concurrencybank.phase4_ledger.dto.CreateAccountRequest;
import com.concurrencybank.phase4_ledger.entity.Account;
import com.concurrencybank.phase4_ledger.exception.AccountNotFoundException;
import com.concurrencybank.phase4_ledger.repository.AccountRepository;
import jakarta.validation.Valid;
import java.util.List;
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

    @PostMapping("/api/accounts")
    @ResponseStatus(HttpStatus.CREATED)
    public AccountResponse create(@Valid @RequestBody CreateAccountRequest request) {
        Account saved = accounts.save(new Account(request.owner(), request.initialBalanceMinor()));
        return AccountResponse.from(saved);
    }

    @GetMapping("/api/accounts")
    public List<AccountResponse> listAll() {
        return accounts.findAll().stream().map(AccountResponse::from).toList();
    }

    @GetMapping("/api/accounts/{id}")
    public AccountResponse getOne(@PathVariable Long id) {
        return accounts.findById(id).map(AccountResponse::from).orElseThrow(() -> new AccountNotFoundException(id));
    }
}

package com.kafkabank.payment;

import com.kafkabank.payment.entity.Account;
import com.kafkabank.payment.repository.AccountRepository;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/** Read side of the payment role — lets a test (or a human) see the effect of a consumed event. */
@RestController
public class AccountController {

  private final AccountRepository accounts;

  public AccountController(AccountRepository accounts) {
    this.accounts = accounts;
  }

  public record AccountView(Long id, String owner, long balanceMinor) {}

  @GetMapping("/api/accounts")
  @Transactional(readOnly = true)
  public List<AccountView> listAll() {
    return accounts.findAll().stream().map(AccountController::toView).toList();
  }

  @GetMapping("/api/accounts/{id}")
  @Transactional(readOnly = true)
  public AccountView getOne(@PathVariable Long id) {
    return accounts
        .findById(id)
        .map(AccountController::toView)
        .orElseThrow(() -> new AccountNotFoundException(id));
  }

  private static AccountView toView(Account account) {
    return new AccountView(account.getId(), account.getOwner(), account.getBalanceMinor());
  }

  @ExceptionHandler(AccountNotFoundException.class)
  public ProblemDetail handleAccountNotFound(AccountNotFoundException e) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
  }
}

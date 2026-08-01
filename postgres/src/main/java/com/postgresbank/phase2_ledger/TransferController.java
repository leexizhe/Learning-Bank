package com.postgresbank.phase2_ledger;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TransferController {

  private final TransferService transfers;

  public TransferController(TransferService transfers) {
    this.transfers = transfers;
  }

  @PostMapping("/api/transfers")
  public TransferResult transfer(@RequestBody TransferRequest request) {
    return transfers.transfer(
        request.idempotencyKey(),
        request.fromAccountId(),
        request.toAccountId(),
        request.amountMinor());
  }

  public record TransferRequest(
      @NotBlank String idempotencyKey,
      long fromAccountId,
      long toAccountId,
      @Positive long amountMinor) {}
}

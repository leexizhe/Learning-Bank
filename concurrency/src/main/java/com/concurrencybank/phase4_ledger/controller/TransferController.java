package com.concurrencybank.phase4_ledger.controller;

import com.concurrencybank.phase4_ledger.dto.TransferRequest;
import com.concurrencybank.phase4_ledger.dto.TransferResponse;
import com.concurrencybank.phase4_ledger.service.TransferService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TransferController {

  private final TransferService transferService;

  public TransferController(TransferService transferService) {
    this.transferService = transferService;
  }

  @PostMapping("/api/transfers")
  public TransferResponse transfer(@Valid @RequestBody TransferRequest request) {
    return transferService.transfer(
        request.fromAccountId(), request.toAccountId(), request.amountMinor());
  }
}

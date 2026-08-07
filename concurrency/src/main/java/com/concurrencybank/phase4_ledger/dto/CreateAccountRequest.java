package com.concurrencybank.phase4_ledger.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.PositiveOrZero;

public record CreateAccountRequest(
        @NotBlank String owner, @PositiveOrZero long initialBalanceMinor) {}

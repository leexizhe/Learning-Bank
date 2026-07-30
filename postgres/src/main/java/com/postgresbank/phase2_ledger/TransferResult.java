package com.postgresbank.phase2_ledger;

public record TransferResult(Long transferId, boolean alreadyApplied) {}

package com.postgresbank.phase1_isolation;

import jakarta.validation.constraints.Positive;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OverdraftController {

    private final JointOverdraftService overdraft;

    public OverdraftController(JointOverdraftService overdraft) {
        this.overdraft = overdraft;
    }

    /** {@code isolation=READ_COMMITTED} reproduces write skew; {@code SERIALIZABLE} prevents it. */
    @PostMapping("/api/overdraft/withdraw")
    public void withdraw(@RequestBody WithdrawRequest request) {
        if (request.isolation() == IsolationChoice.SERIALIZABLE) {
            overdraft.withdrawSerializable(request.debitAccountId(), request.partnerAccountId(), request.amountMinor());
        } else {
            overdraft.withdrawReadCommitted(
                    request.debitAccountId(), request.partnerAccountId(), request.amountMinor());
        }
    }

    public enum IsolationChoice {
        READ_COMMITTED,
        SERIALIZABLE
    }

    public record WithdrawRequest(
            long debitAccountId,
            long partnerAccountId,
            @Positive long amountMinor,
            IsolationChoice isolation) {}
}

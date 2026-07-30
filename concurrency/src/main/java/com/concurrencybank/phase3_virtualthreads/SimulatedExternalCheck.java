package com.concurrencybank.phase3_virtualthreads;

/** The one bit of logic {@code Fraud}/{@code Credit}/{@code Sanctions}CheckClient share: turn a prefix match into a {@link ValidationResult}. Each client still owns its own latency and {@code Thread.sleep} call. */
final class SimulatedExternalCheck {

    private SimulatedExternalCheck() {}

    static ValidationResult decide(
            String checkName, String transactionId, String rejectPrefix, String approvedReason, String rejectedReason) {
        boolean approved = !transactionId.startsWith(rejectPrefix);
        return new ValidationResult(checkName, approved, approved ? approvedReason : rejectedReason);
    }
}

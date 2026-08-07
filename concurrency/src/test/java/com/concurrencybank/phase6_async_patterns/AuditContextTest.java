package com.concurrencybank.phase6_async_patterns;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class AuditContextTest {

    @Test
    void isUnboundOutsideAnyScope() {
        assertThat(AuditContext.currentCorrelationId()).isEmpty();
    }

    @Test
    void correlationIdPropagatesAutomaticallyIntoForkedSubtasks() throws InterruptedException {
        List<String> results = AuditContext.runAuditedAccountChecks("REQ-42");

        assertThat(results)
                .containsExactlyInAnyOrder(
                        "fraud-check saw: REQ-42", "credit-check saw: REQ-42", "sanctions-check saw: REQ-42");
    }

    @Test
    void differentCallsGetIndependentCorrelationIds() throws InterruptedException {
        List<String> first = AuditContext.runAuditedAccountChecks("REQ-1");
        List<String> second = AuditContext.runAuditedAccountChecks("REQ-2");

        assertThat(first).allMatch(r -> r.endsWith("REQ-1"));
        assertThat(second).allMatch(r -> r.endsWith("REQ-2"));
    }
}

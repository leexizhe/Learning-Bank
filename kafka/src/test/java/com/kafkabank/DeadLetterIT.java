package com.kafkabank;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkabank.common.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.kafka.support.KafkaHeaders;

/**
 * What happens to a message that genuinely cannot be processed.
 *
 * <p>The rule that matters in banking: <b>never silently drop it.</b> A
 * dead-lettered record is a payment some customer believes they made. It goes to
 * a dedicated topic, keeping the original topic/offset and the failure reason as
 * headers, so a human can triage it and a replay job can re-drive it once the
 * underlying bug is fixed.
 *
 * <p>An unknown account extends {@code PermanentFailureException}, which
 * {@code @RetryableTopic} excludes, so it skips the retry topics entirely and
 * dead-letters immediately — retrying a permanently-unknown account would just
 * burn the attempts to reach the same conclusion.
 */
class DeadLetterIT extends BaseKafkaIT {

    @Test
    @Timeout(90)
    void anUnprocessableEventLandsInTheDeadLetterTopicWithDiagnosticHeaders() {
        String paymentId = UUID.randomUUID().toString();
        Long missingAccountId = 999_999_999L;

        ConsumerRecord<String, String> dead = awaitRecordMatching(
                Topics.PAYMENT_EVENTS_DLT,
                () -> sendInitiated(paymentId, missingAccountId, 5_000),
                record -> record.value() != null && record.value().contains(paymentId),
                Duration.ofSeconds(60));

        // The headers are the whole point - without them a DLT is just a graveyard
        // you can't do anything with.
        assertThat(header(dead, KafkaHeaders.ORIGINAL_TOPIC)).isEqualTo(Topics.PAYMENT_EVENTS);
        assertThat(header(dead, KafkaHeaders.EXCEPTION_MESSAGE)).contains(String.valueOf(missingAccountId));
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        assertThat(header).as("expected header %s on the DLT record", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}

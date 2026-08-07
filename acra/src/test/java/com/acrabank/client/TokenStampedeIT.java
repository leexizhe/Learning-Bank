package com.acrabank.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.acrabank.AcraTestBase;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * A cold cache hit by many requests at once must produce one authentication, not one per request.
 *
 * <p>The race is made deterministic rather than probable: all 16 threads rendezvous on a barrier and are released
 * together, so they are genuinely inside {@code token()} at the same moment. Starting 16 threads and hoping they
 * overlap would pass just as readily against a completely unsynchronised implementation.
 */
class TokenStampedeIT extends AcraTestBase {

    private static final int THREADS = 16;

    @Autowired
    AcraProfileClient client;

    @Test
    void sixteenThreadsOnAnEmptyCacheProduceOneTokenCall() throws Exception {
        CyclicBarrier startTogether = new CyclicBarrier(THREADS);

        try (ExecutorService pool = Executors.newFixedThreadPool(THREADS)) {
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < THREADS; i++) {
                results.add(pool.submit(() -> {
                    startTogether.await();
                    return client.fetch("16888888A");
                }));
            }
            for (Future<String> result : results) {
                assertThat(result.get()).contains("ABC ENTERPRISE");
            }
        }

        assertThat(ACRA.profileRequests()).isEqualTo(THREADS);
        assertThat(ACRA.tokenRequests())
                .as("the refresh lock must collapse the stampede into a single authentication")
                .isEqualTo(1);
        assertThat(ACRA.presentedTokens()).containsOnly("test-token-1");
    }
}

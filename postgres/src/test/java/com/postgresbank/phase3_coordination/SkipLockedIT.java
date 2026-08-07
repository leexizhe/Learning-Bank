package com.postgresbank.phase3_coordination;

import static org.assertj.core.api.Assertions.assertThat;

import com.postgresbank.TestContainerConfig;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Seeds a batch of jobs and lets several "worker" threads race to claim them via {@code SELECT ... FOR UPDATE SKIP
 * LOCKED}. Proves the two properties that make this pattern scale to multiple service instances: every job is claimed
 * by exactly one worker (no two workers ever process the same row), and no worker ever blocks waiting on a row another
 * worker already holds - the whole batch drains quickly instead of serializing behind lock waits.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SkipLockedIT extends TestContainerConfig {

    @Autowired
    private PaymentJobRepository jobRepository;

    @Autowired
    private JobRunner jobRunner;

    private final ExecutorService pool = Executors.newFixedThreadPool(8);

    @Test
    void everyJobIsClaimedExactlyOnceAcrossConcurrentWorkers() throws Exception {
        String batch = UUID.randomUUID().toString();
        int jobCount = 30;
        jobRepository.saveAll(IntStream.range(0, jobCount)
                .mapToObj(i -> new PaymentJob(batch + "-job-" + i))
                .toList());

        Set<Long> claimed = ConcurrentHashMap.newKeySet();
        Callable<Void> worker = () -> {
            Optional<Long> id;
            while ((id = jobRunner.claimNext()).isPresent()) {
                boolean firstClaim = claimed.add(id.get());
                assertThat(firstClaim)
                        .as("job %s claimed by more than one worker", id.get())
                        .isTrue();
            }
            return null;
        };

        List<Future<Void>> workers =
                IntStream.range(0, 8).mapToObj(i -> pool.submit(worker)).toList();
        for (Future<Void> worker1 : workers) {
            worker1.get(20, TimeUnit.SECONDS);
        }

        assertThat(claimed).as("every seeded job should have been claimed").hasSize(jobCount);
        assertThat(jobRepository.countByStatus("DONE")).isGreaterThanOrEqualTo(jobCount);
    }
}

package com.concurrencybank.phase4_loadbalancer;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Two strategies behind the same interface, so the blocks below deliberately run the same three shapes: a
 * single-threaded selection property, the identical empty-list rejection, and a concurrent stress loop. What each
 * strategy can promise under that stress is where they part company — round-robin distributes <em>exactly</em> evenly
 * because every pick is a distinct atomic index, while random can only promise that no server is starved.
 */
class Phase4LoadBalancerTest {

    private static final List<String> SERVERS = List.of("a", "b", "c", "d");

    @Nested
    class RoundRobinTests {

        @Test
        void cyclesThroughServersInOrder() {
            RoundRobinLoadBalancer balancer = new RoundRobinLoadBalancer();
            List<String> servers = List.of("a", "b", "c");

            assertThat(balancer.pickServer(servers)).isEqualTo("a");
            assertThat(balancer.pickServer(servers)).isEqualTo("b");
            assertThat(balancer.pickServer(servers)).isEqualTo("c");
            assertThat(balancer.pickServer(servers)).isEqualTo("a");
        }

        @Test
        void rejectsEmptyServerList() {
            RoundRobinLoadBalancer balancer = new RoundRobinLoadBalancer();
            assertThatThrownBy(() -> balancer.pickServer(List.of())).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void distributesEvenlyUnderConcurrentLoad() throws InterruptedException {
            RoundRobinLoadBalancer balancer = new RoundRobinLoadBalancer();
            Map<String, Integer> counts = new ConcurrentHashMap<>();
            int threads = 400;
            int callsPerThread = 100;

            runConcurrently(threads, () -> {
                for (int i = 0; i < callsPerThread; i++) {
                    counts.merge(balancer.pickServer(SERVERS), 1, Integer::sum);
                }
            });

            int total = threads * callsPerThread;
            assertThat(counts.values().stream().mapToInt(Integer::intValue).sum())
                    .isEqualTo(total);
            // Perfectly even (total / 4) since every pick is a distinct atomic index.
            int expectedPerServer = total / SERVERS.size();
            counts.values().forEach(count -> assertThat(count).isEqualTo(expectedPerServer));
        }
    }

    @Nested
    class RandomTests {

        @Test
        void alwaysPicksFromTheGivenServerList() {
            RandomLoadBalancer balancer = new RandomLoadBalancer();
            List<String> servers = List.of("a", "b", "c");

            for (int i = 0; i < 1_000; i++) {
                assertThat(servers).contains(balancer.pickServer(servers));
            }
        }

        @Test
        void rejectsEmptyServerList() {
            RandomLoadBalancer balancer = new RandomLoadBalancer();
            assertThatThrownBy(() -> balancer.pickServer(List.of())).isInstanceOf(IllegalArgumentException.class);
        }

        /**
         * The weaker sibling of {@link RoundRobinTests#distributesEvenlyUnderConcurrentLoad()}: no exact counts to
         * assert, only that every server was reached at least once. Randomness cannot promise a distribution, so the
         * assertion has to be the weakest claim that would still fail if picking were broken.
         */
        @Test
        void everyServerGetsPickedUnderConcurrentLoad() throws InterruptedException {
            RandomLoadBalancer balancer = new RandomLoadBalancer();
            Set<String> seen = ConcurrentHashMap.newKeySet();

            runConcurrently(200, () -> {
                for (int i = 0; i < 50; i++) {
                    seen.add(balancer.pickServer(SERVERS));
                }
            });

            assertThat(seen).containsExactlyInAnyOrderElementsOf(SERVERS);
        }
    }
}

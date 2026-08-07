package com.concurrencybank.phase4_loadbalancer;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class RoundRobinLoadBalancerTest {

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
        List<String> servers = List.of("a", "b", "c", "d");
        Map<String, Integer> counts = new ConcurrentHashMap<>();
        int threads = 400;
        int callsPerThread = 100;

        runConcurrently(threads, () -> {
            for (int i = 0; i < callsPerThread; i++) {
                counts.merge(balancer.pickServer(servers), 1, Integer::sum);
            }
        });

        int total = threads * callsPerThread;
        assertThat(counts.values().stream().mapToInt(Integer::intValue).sum()).isEqualTo(total);
        // Perfectly even (total / 4) since every pick is a distinct atomic index.
        int expectedPerServer = total / servers.size();
        counts.values().forEach(count -> assertThat(count).isEqualTo(expectedPerServer));
    }
}

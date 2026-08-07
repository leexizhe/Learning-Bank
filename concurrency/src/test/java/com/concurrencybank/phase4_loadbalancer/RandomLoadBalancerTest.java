package com.concurrencybank.phase4_loadbalancer;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class RandomLoadBalancerTest {

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

    @Test
    void everyServerGetsPickedUnderConcurrentLoad() throws InterruptedException {
        RandomLoadBalancer balancer = new RandomLoadBalancer();
        List<String> servers = List.of("a", "b", "c", "d");
        Set<String> seen = ConcurrentHashMap.newKeySet();

        runConcurrently(200, () -> {
            for (int i = 0; i < 50; i++) {
                seen.add(balancer.pickServer(servers));
            }
        });

        assertThat(seen).containsExactlyInAnyOrderElementsOf(servers);
    }
}

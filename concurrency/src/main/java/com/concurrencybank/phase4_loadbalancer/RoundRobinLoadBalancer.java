package com.concurrencybank.phase4_loadbalancer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The pointer to "next server" is shared, mutable state read and updated by
 * every calling thread, so it has to be an {@link AtomicInteger} rather than a
 * plain {@code int} — {@code getAndIncrement} is the atomic read-then-advance
 * that a plain field can't give you under concurrent calls. Wrapping with
 * {@code Math.floorMod} instead of {@code %} keeps the index non-negative once
 * the counter itself wraps around after ~2^31 calls.
 */
public class RoundRobinLoadBalancer implements LoadBalancer {

    private final AtomicInteger next = new AtomicInteger();

    @Override
    public String pickServer(List<String> servers) {
        if (servers.isEmpty()) {
            throw new IllegalArgumentException("No servers available");
        }
        int index = Math.floorMod(next.getAndIncrement(), servers.size());
        return servers.get(index);
    }
}

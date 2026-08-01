package com.concurrencybank.phase4_loadbalancer;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * {@link ThreadLocalRandom#current()} rather than a shared {@link java.util.Random}: a shared
 * {@code Random} instance CAS-loops internally on every call, so it becomes a contention point
 * under many concurrent callers. Each thread gets its own generator here, so there's nothing to
 * contend over.
 */
public class RandomLoadBalancer implements LoadBalancer {

  @Override
  public String pickServer(List<String> servers) {
    if (servers.isEmpty()) {
      throw new IllegalArgumentException("No servers available");
    }
    int index = ThreadLocalRandom.current().nextInt(servers.size());
    return servers.get(index);
  }
}

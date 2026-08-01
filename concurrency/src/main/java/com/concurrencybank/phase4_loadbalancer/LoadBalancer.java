package com.concurrencybank.phase4_loadbalancer;

import java.util.List;

public interface LoadBalancer {

  /**
   * @throws IllegalArgumentException if servers is empty
   */
  String pickServer(List<String> servers);
}

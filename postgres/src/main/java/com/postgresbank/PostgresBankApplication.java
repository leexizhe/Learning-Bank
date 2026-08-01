package com.postgresbank;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** {@code @EnableScheduling} drives phase3_coordination's OutboxRelay poller. */
@EnableScheduling
@SpringBootApplication
public class PostgresBankApplication {

  public static void main(String[] args) {
    SpringApplication.run(PostgresBankApplication.class, args);
  }
}

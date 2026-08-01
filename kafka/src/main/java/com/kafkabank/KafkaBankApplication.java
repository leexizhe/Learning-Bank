package com.kafkabank;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * {@code @EnableScheduling} is load-bearing: without it {@code PaymentOutboxRelay}'s
 * {@code @Scheduled} sweep is never invoked, and the outbox is correct only for as long as the
 * after-commit shortcut keeps working. The failure would be silent.
 */
@EnableScheduling
@SpringBootApplication
public class KafkaBankApplication {

  public static void main(String[] args) {
    SpringApplication.run(KafkaBankApplication.class, args);
  }
}

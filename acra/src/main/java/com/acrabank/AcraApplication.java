package com.acrabank;

import java.time.Clock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@ConfigurationPropertiesScan
public class AcraApplication {

    public static void main(String[] args) {
        SpringApplication.run(AcraApplication.class, args);
    }

    /**
     * Every "is this expired yet" decision in this module reads the clock through this bean rather than calling {@code
     * Instant.now()} inline, so a test can prove that a 1799-second token is refreshed at 1739 seconds by *advancing a
     * clock* instead of sleeping for half an hour. See MutableClock in the test sources.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}

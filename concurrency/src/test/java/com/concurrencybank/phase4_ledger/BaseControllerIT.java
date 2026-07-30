package com.concurrencybank.phase4_ledger;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
abstract class BaseControllerIT extends TestContainerConfig {

    @LocalServerPort
    private int port;

    final TestRestTemplate rest = new TestRestTemplate();

    final String baseUrl() {
        return "http://localhost:" + port;
    }
}

package com.concurrencybank.phase4_ledger;

import static org.assertj.core.api.Assertions.assertThat;

import com.concurrencybank.phase4_ledger.dto.AccountResponse;
import com.concurrencybank.phase4_ledger.dto.CreateAccountRequest;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class AccountControllerIT extends BaseControllerIT {

    @Test
    void createsAndFetchesAnAccount() {
        String owner = "owner-" + UUID.randomUUID();
        ResponseEntity<AccountResponse> created = rest.postForEntity(
                baseUrl() + "/api/accounts", new CreateAccountRequest(owner, 5_000), AccountResponse.class);

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.getBody()).isNotNull();
        assertThat(created.getBody().owner()).isEqualTo(owner);
        assertThat(created.getBody().balanceMinor()).isEqualTo(5_000);

        ResponseEntity<AccountResponse> fetched = rest.getForEntity(
                baseUrl() + "/api/accounts/" + created.getBody().id(), AccountResponse.class);
        assertThat(fetched.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(fetched.getBody().balanceMinor()).isEqualTo(5_000);
    }

    @Test
    void returnsNotFoundForUnknownAccount() {
        ResponseEntity<String> response = rest.getForEntity(baseUrl() + "/api/accounts/999999999", String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}

package com.concurrencybank.phase4_ledger;

import static com.concurrencybank.testutil.ConcurrencyHarness.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;

import com.concurrencybank.phase4_ledger.dto.AccountResponse;
import com.concurrencybank.phase4_ledger.dto.CreateAccountRequest;
import com.concurrencybank.phase4_ledger.dto.TransferRequest;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;

/**
 * The phase's integration tests, against a real Spring MVC stack and a real Postgres (Testcontainers). The Spring and
 * container wiring is inherited rather than declared here — see {@link BaseControllerIT} and
 * {@link TestContainerConfig} — so the blocks below can be read as plain HTTP exercises.
 *
 * <p>{@link AccountControllerTests} covers the boring half that has to work before anything else can be trusted;
 * {@link TransferControllerTests} is the centerpiece.
 */
class Phase4LedgerIT extends BaseControllerIT {

    @Nested
    class AccountControllerTests {

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

    /**
     * Never inspects locks or transactions directly, it just proves the observable guarantee. 200 real HTTP transfer
     * requests hit a real Postgres concurrently, alternating direction between the same two accounts — exactly the
     * deadlock trap shape from {@code phase2_deadlock}, now exercised through the full Spring MVC + JPA stack. If
     * {@code TransferService}'s lock ordering were wrong, this would either hang (deadlock) or leave the books
     * unbalanced (lost update).
     */
    @Nested
    class TransferControllerTests {

        private AccountResponse createAccount(String owner, long balanceMinor) {
            ResponseEntity<AccountResponse> response = rest.postForEntity(
                    baseUrl() + "/api/accounts", new CreateAccountRequest(owner, balanceMinor), AccountResponse.class);
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            return response.getBody();
        }

        /**
         * Response type is {@code String}, not {@code TransferResponse}: on the error paths the body is a ProblemDetail
         * JSON object, which can't be deserialized into {@code TransferResponse}'s primitive fields. Reading the raw
         * body avoids a message-converter exception masking the assertion this test actually cares about (the status
         * code).
         */
        private ResponseEntity<String> postTransfer(Long fromId, Long toId, long amountMinor) {
            return rest.postForEntity(
                    baseUrl() + "/api/transfers", new TransferRequest(fromId, toId, amountMinor), String.class);
        }

        @Test
        void concurrentOppositeDirectionTransfersStayBalanced() throws InterruptedException {
            long initialBalance = 1_000_000;
            AccountResponse alice = createAccount("alice-" + UUID.randomUUID(), initialBalance);
            AccountResponse bob = createAccount("bob-" + UUID.randomUUID(), initialBalance);

            int requestsPerDirection = 100;
            long amountPerTransfer = 10;

            AtomicInteger nextRequest = new AtomicInteger();
            List<HttpStatusCode> results = new CopyOnWriteArrayList<>();

            runConcurrently(requestsPerDirection * 2, () -> {
                boolean aliceToBob = nextRequest.getAndIncrement() % 2 == 0;
                ResponseEntity<String> response = aliceToBob
                        ? postTransfer(alice.id(), bob.id(), amountPerTransfer)
                        : postTransfer(bob.id(), alice.id(), amountPerTransfer);
                results.add(response.getStatusCode());
            });

            assertThat(results).allMatch(status -> status.equals(HttpStatus.OK));

            AccountResponse aliceAfter = rest.getForEntity(
                            baseUrl() + "/api/accounts/" + alice.id(), AccountResponse.class)
                    .getBody();
            AccountResponse bobAfter = rest.getForEntity(baseUrl() + "/api/accounts/" + bob.id(), AccountResponse.class)
                    .getBody();

            // Equal counts of $10 transfers in each direction net out to zero.
            assertThat(aliceAfter.balanceMinor()).isEqualTo(initialBalance);
            assertThat(bobAfter.balanceMinor()).isEqualTo(initialBalance);
            assertThat(aliceAfter.balanceMinor() + bobAfter.balanceMinor()).isEqualTo(2 * initialBalance);
        }

        @Test
        void rejectsTransferBelowAvailableBalance() {
            AccountResponse from = createAccount("poor-" + UUID.randomUUID(), 50);
            AccountResponse to = createAccount("rich-" + UUID.randomUUID(), 0);

            ResponseEntity<String> response = postTransfer(from.id(), to.id(), 100);

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        }

        @Test
        void returnsNotFoundWhenAnAccountDoesNotExist() {
            AccountResponse from = createAccount("solo-" + UUID.randomUUID(), 100);

            ResponseEntity<String> response = postTransfer(from.id(), 999_999_999L, 10);

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        }
    }
}

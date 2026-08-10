package com.acrabank;

import static org.assertj.core.api.Assertions.assertThat;

import com.acrabank.entity.BusinessProfile;
import com.acrabank.repository.BusinessProfileRepository;
import com.acrabank.service.BusinessProfileService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The only test that talks to the real ACRA sandbox, and the only one that can tell you the rest of
 * the suite is testing the right contract. Everything else runs against recorded responses, so
 * every one of them would keep passing if ACRA renamed a field tomorrow.
 *
 * <p>Skipped unless {@code ACRA_CLIENT_ID} is set, which means skipped in CI and on any machine
 * without credentials. Run it deliberately:
 *
 * <pre>
 *   $env:ACRA_CLIENT_ID = "..."; $env:ACRA_CLIENT_SECRET = "..."
 *   .\mvnw.cmd -pl acra verify -Dit.test=SandboxSmokeIT
 * </pre>
 *
 * <p>It prints the field names ACRA actually returned. That output is the input to trimming {@code
 * ProfileMapper}'s candidate pointer lists down to the ones that are real.
 */
@SpringBootTest
@EnabledIfEnvironmentVariable(named = "ACRA_CLIENT_ID", matches = ".+")
class SandboxSmokeIT {

    private static final String SANDBOX_UEN = "16888888A";

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        // The container comes from the shared base class; only the ACRA endpoint differs.
        registry.add("spring.datasource.url", AcraTestBase.POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", AcraTestBase.POSTGRES::getUsername);
        registry.add("spring.datasource.password", AcraTestBase.POSTGRES::getPassword);
        registry.add("acra.base-url", () -> "https://api-sandbox.bizfile.gov.sg");
    }

    @Autowired
    BusinessProfileService service;

    @Autowired
    BusinessProfileRepository profiles;

    @Autowired
    ObjectMapper json;

    /**
     * Asserts the contract the offline suite assumes: the entities envelope, and the four field names {@code
     * ProfileMapper} promotes. If ACRA renames any of them, this is the only test in the repository that will notice.
     */
    @Test
    void fetchesAndStoresARealBusinessProfile() throws Exception {
        profiles.deleteById(SANDBOX_UEN);

        BusinessProfile stored = service.get(SANDBOX_UEN, true);

        JsonNode payload = json.readTree(stored.getPayload());
        assertThat(payload.at("/entities").isArray())
                .as("the entities envelope")
                .isTrue();
        assertThat(payload.at("/entities/0/uen").asText()).isEqualTo(SANDBOX_UEN);

        assertThat(stored.getEntityName()).as("entityName still promotes").isNotBlank();
        assertThat(stored.getEntityStatus())
                .as("statusOfBusiness still promotes")
                .isNotBlank();
        assertThat(stored.getEntityType())
                .as("constitutionOfBusiness still promotes")
                .isNotBlank();
        assertThat(stored.getRegistrationDate())
                .as("registrationDate still parses")
                .isNotNull();

        System.out.println("=== ACRA sandbox response for " + SANDBOX_UEN + " ===");
        System.out.println(json.writerWithDefaultPrettyPrinter().writeValueAsString(payload));
        System.out.println("=== promoted: name="
                + stored.getEntityName()
                + " status="
                + stored.getEntityStatus()
                + " type="
                + stored.getEntityType()
                + " registered="
                + stored.getRegistrationDate()
                + " ===");
    }
}

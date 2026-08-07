package com.acrabank.profile;

import static org.assertj.core.api.Assertions.assertThat;

import com.acrabank.AcraTestBase;
import com.acrabank.testsupport.FakeAcraServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** What actually lands in Postgres. */
class ProfilePersistenceIT extends AcraTestBase {

    @Autowired
    BusinessProfileService service;

    @Autowired
    ObjectMapper json;

    @Test
    void storesTheWholeResponseAndPromotesTheIndexedFields() throws Exception {
        service.get("16888888A", false);

        assertThat(profiles.count()).isEqualTo(1);
        BusinessProfile stored = profiles.findById("16888888A").orElseThrow();

        assertThat(stored.getEntityName()).isEqualTo("ABC ENTERPRISE");
        assertThat(stored.getEntityStatus()).isEqualTo("LIVE");
        assertThat(stored.getEntityType()).isEqualTo("SOLE-PROPRIETOR");
        assertThat(stored.getRegistrationDate()).isEqualTo(LocalDate.of(2016, 8, 18));
        assertThat(stored.getFetchedAt()).isEqualTo(T0);

        // Semantic equality, not byte equality - and the difference is the point. jsonb is a parsed representation:
        // Postgres normalises whitespace and does not keep key order, so asserting on the text would be asserting on an
        // implementation detail of the storage type rather than on the data surviving the round trip.
        assertThat(json.readTree(stored.getPayload())).isEqualTo(json.readTree(FakeAcraServer.SAMPLE_PROFILE));
    }

    @Test
    void anEntityWithUnrecognisedFieldNamesStillStoresItsPayloadWhole() throws Exception {
        // The case that matters if ACRA renames its fields: the promoted columns go null, a warning is logged, and not
        // one byte of the response is lost. That is the entire justification for keeping the payload as jsonb rather
        // than shredding it into typed columns.
        String renamed = """
        {"entities":[{"uen":"20999999Z","businessName":"MYSTERY PTE LTD","someFutureField":42}]}
        """;
        ACRA.defaultProfileResponse(200, renamed);

        service.get("20999999Z", false);

        BusinessProfile stored = profiles.findById("20999999Z").orElseThrow();
        assertThat(stored.getEntityName()).isNull();
        assertThat(stored.getEntityStatus()).isNull();
        assertThat(stored.getRegistrationDate()).isNull();
        assertThat(json.readTree(stored.getPayload())).isEqualTo(json.readTree(renamed));
    }

    @Test
    void storesTheNestedStructuresThatHaveNoColumns() throws Exception {
        // Addresses, activity codes and representatives are never mapped to columns, so this is the assertion that they
        // survive the trip at all.
        service.get("16888888A", false);

        JsonNode entity = json.readTree(
                        profiles.findById("16888888A").orElseThrow().getPayload())
                .at("/entities/0");

        assertThat(entity.at("/principalPlaceOfBusiness/buildingName").asText()).isEqualTo("ABC BUILDING");
        assertThat(entity.at("/primaryActivity/code").asText()).isEqualTo("47112");
        assertThat(entity.at("/authorisedRepresentative/principalName").asText())
                .isEqualTo("NG AH MEI");
        assertThat(entity.at("/partner/position").asText()).isEqualTo("OWNER");
    }

    @Test
    void aRefetchUpdatesTheRowInsteadOfInsertingASecondOne() {
        service.get("16888888A", false);
        clock.advance(Duration.ofDays(30));
        service.get("16888888A", true);

        assertThat(profiles.count()).as("the UEN is the primary key").isEqualTo(1);
        assertThat(profiles.findById("16888888A").orElseThrow().getFetchedAt()).isEqualTo(T0.plus(Duration.ofDays(30)));
    }

    @Test
    void differentUensGetDifferentRows() {
        service.get("16888888A", false);
        service.get("20999999Z", false);

        assertThat(profiles.count()).isEqualTo(2);
    }
}

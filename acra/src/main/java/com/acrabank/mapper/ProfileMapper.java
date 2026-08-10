package com.acrabank.mapper;

import com.acrabank.entity.BusinessProfile;
import com.acrabank.exception.AcraApiException;
import com.acrabank.exception.ProfileNotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Understands the one thing in this module that is specific to ACRA's response shape.
 *
 * <p>The shape below was read off a live sandbox call rather than documentation, which does not
 * appear to exist publicly. A business profile arrives wrapped in an {@code entities} array:
 *
 * <pre>
 * {"entities": [{
 *    "uen": "16888888A",
 *    "entityName": "ABC ENTERPRISE",
 *    "registrationDate": "2016-08-18",
 *    "statusOfBusiness": "LIVE",
 *    "constitutionOfBusiness": "SOLE-PROPRIETOR",
 *    "principalPlaceOfBusiness": {...}, "primaryActivity": {...}, ...
 * }]}
 * </pre>
 *
 * <p>Only the fields promoted to columns are named here. Everything else - addresses, activity
 * codes, representatives, and whatever a company profile carries that this sole-proprietorship
 * sample does not - stays in the stored payload and needs no code at all.
 *
 * <p>Promotion is still allowed to fail softly: a renamed field costs a null column and a warning,
 * never a failed ingest, because the payload is stored whole either way. Failing to *find an
 * entity* is different, and is an error - see {@link #requireEntity}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ProfileMapper {

    private static final String ENTITIES = "/entities";
    private static final String NAME = "/entities/0/entityName";
    private static final String STATUS = "/entities/0/statusOfBusiness";
    private static final String CONSTITUTION = "/entities/0/constitutionOfBusiness";
    private static final String REGISTRATION_DATE = "/entities/0/registrationDate";

    // The sandbox returns ISO dates ("2016-08-18") on every date field. The second format is a hedge, not an
    // observation - Singapore government APIs disagree about this often enough that one spare parse is cheaper than a
    // null column.
    private static final List<DateTimeFormatter> DATE_FORMATS =
            List.of(DateTimeFormatter.ISO_LOCAL_DATE, DateTimeFormatter.ofPattern("dd/MM/uuuu"));

    private final ObjectMapper json;

    public JsonNode parse(String payload) {
        try {
            return json.readTree(payload);
        } catch (JsonProcessingException e) {
            throw new AcraApiException(200, "response body was not valid JSON: " + e.getOriginalMessage());
        }
    }

    /**
     * ACRA answers an unknown UEN with {@code 200 {"entities":[]}}, not a 404 - so the HTTP status is not enough to
     * tell "no such company" from "here is the company". Without this check a mistyped UEN would be stored as a
     * perfectly valid row containing nothing, and then served from cache for a week.
     */
    public void requireEntity(JsonNode root, String uen) {
        JsonNode entities = root.at(ENTITIES);
        if (!entities.isArray() || entities.isEmpty()) {
            throw new ProfileNotFoundException(uen);
        }
        if (entities.size() > 1) {
            // Never seen, but a UEN is supposed to identify exactly one entity. If this ever fires, the "take the
            // first" assumption below needs revisiting.
            log.warn("ACRA returned {} entities for UEN {}; using the first", entities.size(), uen);
        }
    }

    /** Writes the payload and whatever could be promoted onto {@code target}. */
    public void apply(BusinessProfile target, String payload, JsonNode root, Instant fetchedAt) {
        String name = text(root, NAME).orElse(null);
        if (name == null) {
            log.warn(
                    "no entityName in the ACRA payload for UEN {} - promoted columns will be sparse."
                            + " Entity fields present: {}",
                    target.getUen(),
                    fieldNames(root.at("/entities/0")));
        }

        target.refresh(
                payload,
                name,
                text(root, STATUS).orElse(null),
                text(root, CONSTITUTION).orElse(null),
                date(root, REGISTRATION_DATE).orElse(null),
                fetchedAt);
    }

    private Optional<String> text(JsonNode root, String pointer) {
        JsonNode node = root.at(pointer);
        if (node.isValueNode() && !node.asText().isBlank()) {
            return Optional.of(node.asText().trim());
        }
        return Optional.empty();
    }

    private Optional<LocalDate> date(JsonNode root, String pointer) {
        Optional<String> raw = text(root, pointer);
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        for (DateTimeFormatter format : DATE_FORMATS) {
            try {
                return Optional.of(LocalDate.parse(raw.get(), format));
            } catch (DateTimeParseException ignored) {
                // Try the next format.
            }
        }
        log.warn("could not parse '{}' at {} as a date", raw.get(), pointer);
        return Optional.empty();
    }

    private List<String> fieldNames(JsonNode node) {
        List<String> names = new java.util.ArrayList<>();
        node.fieldNames().forEachRemaining(names::add);
        return names;
    }
}

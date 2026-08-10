package com.acrabank.dto;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.time.LocalDate;

/**
 * What {@code /api/profiles/{uen}} answers with: the promoted columns as named fields, plus the whole stored ACRA
 * response.
 *
 * <p>{@code payload} is a {@link JsonNode} rather than the {@code String} it is stored as, so it nests as real JSON in
 * the response instead of arriving as one escaped blob.
 */
public record ProfileResponse(
        String uen,
        String entityName,
        String entityStatus,
        String entityType,
        LocalDate registrationDate,
        Instant fetchedAt,
        JsonNode payload) {}

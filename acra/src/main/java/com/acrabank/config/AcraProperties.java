package com.acrabank.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Bound from the {@code acra.*} block in application.yml. The credentials come from environment variables with no
 * fallback value, so a misconfigured deployment fails at startup rather than at the first request.
 *
 * @param baseUrl the API host; pointed at a local server by the integration tests
 * @param clientId OAuth client id, from {@code ACRA_CLIENT_ID}
 * @param clientSecret OAuth client secret, from {@code ACRA_CLIENT_SECRET}
 * @param profileTtl how stale a stored profile may be before a read re-fetches it
 * @param expiryFallback token lifetime assumed when the response omits {@code expires_in}
 * @param expirySkew how far before the stated expiry to give up on a cached token
 * @param requestTimeout connect and read timeout for both ACRA calls
 */
@ConfigurationProperties("acra")
public record AcraProperties(
        String baseUrl,
        String clientId,
        String clientSecret,
        Duration profileTtl,
        Duration expiryFallback,
        Duration expirySkew,
        Duration requestTimeout) {}

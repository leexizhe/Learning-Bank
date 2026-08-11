package com.acra.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Bound from the {@code acra.*} block in application.yml. */
@ConfigurationProperties("acra")
public record AcraProperties(String baseUrl, String clientId, String clientSecret) {}

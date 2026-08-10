package com.acrabank.exception;

import lombok.Getter;

/** ACRA answered, but not with something usable. Surfaced to callers as a 502, not a 500. */
@Getter
public class AcraApiException extends RuntimeException {

    private final int upstreamStatus;

    public AcraApiException(int upstreamStatus, String message) {
        super("ACRA returned " + upstreamStatus + ": " + message);
        this.upstreamStatus = upstreamStatus;
    }
}

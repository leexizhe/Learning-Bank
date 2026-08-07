package com.acrabank.client;

/** ACRA has no business profile for this UEN. Distinct from "ACRA is broken" - a 404, not a 502. */
public class ProfileNotFoundException extends RuntimeException {

    public ProfileNotFoundException(String uen) {
        super("no ACRA business profile for UEN " + uen);
    }
}

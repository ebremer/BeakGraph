package com.ebremer.beakgraph.cmdline;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

/**
 * Validates {@code -export}: NT, NQ, JSON-LD (or JSONLD), TTL, or TRIG,
 * case-insensitively.
 */
public class ExportFormatValidator implements IParameterValidator {

    /** Canonical form (NT/NQ/JSONLD/TTL/TRIG), or null if unrecognized. */
    static String normalize(String value) {
        if (value == null) return null;
        return switch (value.trim().toUpperCase().replace("-", "")) {
            case "NT" -> "NT";
            case "NQ" -> "NQ";
            case "JSONLD" -> "JSONLD";
            case "TTL" -> "TTL";
            case "TRIG" -> "TRIG";
            default -> null;
        };
    }

    @Override
    public void validate(String name, String value) throws ParameterException {
        if (normalize(value) == null) {
            throw new ParameterException("Parameter " + name
                    + " must be NT, NQ, JSON-LD, TTL, or TRIG; found \"" + value + "\"");
        }
    }
}

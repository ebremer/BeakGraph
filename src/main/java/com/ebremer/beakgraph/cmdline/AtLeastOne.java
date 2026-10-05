package com.ebremer.beakgraph.cmdline;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

/**
 * An integer of at least 1. JCommander's {@code PositiveInteger} accepts 0,
 * so {@code -threads 0} reached {@code new ThreadPoolExecutor(0, 0, ...)}
 * and died with an IllegalArgumentException stack trace (BG-155).
 */
public class AtLeastOne implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
        long n;
        try {
            n = Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new ParameterException("Parameter " + name + " must be a whole number of at least 1; found \"" + value + "\"");
        }
        if (n < 1) {
            throw new ParameterException("Parameter " + name + " must be at least 1; found " + n);
        }
    }
}

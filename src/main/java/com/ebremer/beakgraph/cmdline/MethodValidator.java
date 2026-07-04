package com.ebremer.beakgraph.cmdline;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

/**
 * Validates {@code -method}: 0 = sequential in-memory, 1 = disk-based (huge),
 * 2 = parallel in-memory, 3 = ultra in-memory, 4 = parallel disk-based
 * (hugeUltra).
 */
public class MethodValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
        boolean ok;
        try {
            int v = Integer.parseInt(value);
            ok = v >= 0 && v <= 4;
        } catch (NumberFormatException e) {
            ok = false;
        }
        if (!ok) {
            throw new ParameterException("Parameter " + name + " must be 0 (in-memory), 1 (disk), "
                    + "2 (parallel), 3 (ultra), or 4 (hugeUltra disk); found \"" + value + "\"");
        }
    }
}

package com.ebremer.beakgraph.cmdline;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

/** {@code -port} must be a TCP port, 0..65535 (0 = an ephemeral port); it used to reach Jetty unchecked (BG-155). */
public class PortRange implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
        int port;
        try {
            port = Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new ParameterException("Parameter " + name + " must be a TCP port (0-65535); found \"" + value + "\"");
        }
        if (port < 0 || port > 65535) {
            throw new ParameterException("Parameter " + name + " must be a TCP port (0-65535); found " + port);
        }
    }
}

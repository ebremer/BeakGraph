package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * {@link NodeValues} makes values of Jena's {@code NodeValue} subclasses
 * itself, and can be the first code in a JVM to touch them - on several
 * threads at once, when a build's first sort is what asks. Jena's
 * {@code NodeValue} makes values of those same subclasses while it is
 * initialized, so a thread initializing a subclass and a thread initializing
 * {@code NodeValue} wait for each other: every {@code -method 1} build hung
 * in its first sort until {@code NodeValues} initialized {@code NodeValue}
 * before anything else. A test in this JVM cannot show that - some test
 * before it has made a value - so the first use is run in JVMs of their own.
 */
class NodeValuesFirstUseTest {

    @Test
    void theFirstValuesOfAJvmAreMadeOnManyThreadsAtOnce() throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        for (int run = 0; run < 4; run++) {
            Process jvm = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), NodeValuesFirstUse.class.getName())
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .start();
            boolean ended = jvm.waitFor(120, TimeUnit.SECONDS);
            if (!ended) {
                jvm.destroyForcibly();
            }
            assertTrue(ended, "run " + run + ": the threads making a JVM's first values wait for each other");
            assertEquals(0, jvm.exitValue(), "run " + run + ": every thread made its values");
        }
    }
}

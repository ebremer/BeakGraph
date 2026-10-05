package com.ebremer.beakgraph.core.fuseki;

/**
 * Former home of the resolver; it moved to {@link com.ebremer.beakgraph.core.RelativeIRIResolver}
 * so the programmatic API can use it (BG-396). Kept as an alias for existing callers.
 *
 * @deprecated use {@link com.ebremer.beakgraph.core.RelativeIRIResolver}
 */
@Deprecated
public class RelativeIRIResolver extends com.ebremer.beakgraph.core.RelativeIRIResolver {

    public RelativeIRIResolver(String baseURI) {
        super(baseURI);
    }
}

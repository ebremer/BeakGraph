package com.ebremer.beakgraph.hdf5.readers;

import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import io.jhdf.api.WritableGroup;
import io.jhdf.api.WritableNode;
import java.io.File;
import java.util.Map;

/**
 * Re-emits a store through jHDF's write API with one or more attributes
 * rewritten on the way (jHDF cannot modify a file in place and its object
 * headers are checksummed). Every group, dataset and attribute is copied
 * verbatim except what {@link Rewrite} changes - the way the reader-side
 * tests fabricate files that no BeakGraph writer would produce.
 */
final class StoreCopies {

    private StoreCopies() {}

    /** {@code objectPath} is the jHDF path of the group or dataset carrying the attribute, without a trailing slash. */
    interface Rewrite {
        Object apply(String objectPath, String attribute, Object value);
    }

    static File copy(File src, File dst, Rewrite rewrite) throws Exception {
        try (HdfFile in = new HdfFile(src.toPath()); WritableHdfFile out = HdfFile.write(dst.toPath())) {
            copyChildren(in, out, rewrite);
        }
        return dst;
    }

    private static void copyChildren(Group from, WritableGroup to, Rewrite rewrite) {
        for (Node child : from.getChildren().values()) {
            if (child instanceof Group g) {
                WritableGroup wg = to.putGroup(g.getName());
                copyAttributes(g, wg, rewrite);
                copyChildren(g, wg, rewrite);
            } else if (child instanceof Dataset d) {
                WritableNode wd = to.putDataset(d.getName(), d.getData());
                copyAttributes(d, wd, rewrite);
            }
        }
    }

    private static void copyAttributes(Node from, WritableNode to, Rewrite rewrite) {
        String path = from.getPath().replaceAll("/+$", "");
        for (Map.Entry<String, Attribute> e : from.getAttributes().entrySet()) {
            to.putAttribute(e.getKey(), rewrite.apply(path, e.getKey(), e.getValue().getData()));
        }
    }
}

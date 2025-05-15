package com.ebremer.beakgraph.hdf5.writers;

import static com.ebremer.beakgraph.Params.BLOCKSIZE;
import static com.ebremer.beakgraph.Params.SUPERBLOCKSIZE;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import io.jhdf.api.WritableGroup;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

public class BGIndex {

    private final BitPackedUnSignedLongBuffer B1, B2, B3;
    private final BitPackedUnSignedLongBuffer S1, S2, S3;
    private final BitPackedUnSignedLongBuffer SB1, SB2, SB3;
    private final BitPackedUnSignedLongBuffer BB1, BB2, BB3;
    private final Index type;
    private final IndexPosition[] positions;

    private static class IndexPosition {
        final char component;
        final String name;

        IndexPosition(char component) {
            this.component = component;
            this.name = switch (component) {
                case 'G' -> "g";
                case 'S' -> "s";
                case 'P' -> "p";
                case 'O' -> "o";
                default -> throw new IllegalStateException("Unknown component: " + component);
            };
        }

        Node getNode(Quad quad) {
            return switch (component) {
                case 'G' -> quad.getGraph();
                case 'S' -> quad.getSubject();
                case 'P' -> quad.getPredicate();
                case 'O' -> quad.getObject();
                default -> throw new IllegalStateException();
            };
        }

        long locateInDictionary(FiveSectionDictionaryWriter w, Quad quad) {
            return switch (component) {
                case 'G' -> w.locateGraph(quad.getGraph());
                case 'S' -> w.locateSubject(quad.getSubject());
                case 'P' -> w.locatePredicate(quad.getPredicate());
                case 'O' -> w.locateObject(quad.getObject());
                default -> throw new IllegalStateException();
            };
        }

        int getBitSize(FiveSectionDictionaryWriter w) {
            int needed = switch (component) {
                case 'G' -> MinBits(w.getNumberOfGraphs());
                case 'S' -> MinBits(w.getNumberOfShared() + w.getNumberOfSubjects());
                case 'P' -> MinBits(w.getNumberOfPredicates());
                case 'O' -> MinBits(w.getNumberOfShared() + w.getNumberOfObjects());
                default -> throw new IllegalStateException();
            };
            // This prevents bit-packing corruption for non-aligned widths (e.g. 9 bits)
            // and neutralizes endianness issues during HDF5 I/O.
            if (needed == 0) return 8;
            return (int) (Math.ceil(needed / 8.0) * 8);
        }
    }

    private static class LevelState {
        long bitsProcessed = 0;
        long onesSoFar = 0;
        long onesInCurrentSuperblock = 0;
        long onesInCurrentBlock = 0;
        long lastSuperblockWritten = 0; 
        long lastBlockWritten = -1;     
    }

    public BGIndex(HDF5Writer.Builder builder, FiveSectionDictionaryWriter dictWriter, Index type, Quad[] allQuads) {
        System.out.println("Creating Index " + type);
        this.type = type;
        String indexName = type.name();
        
        this.positions = new IndexPosition[]{
            new IndexPosition(indexName.charAt(0)), 
            new IndexPosition(indexName.charAt(1)), 
            new IndexPosition(indexName.charAt(2)), 
            new IndexPosition(indexName.charAt(3)) 
        };

        int sbBits = MinBits(dictWriter.getNumberOfQuads() + 128); 
        sbBits = (int) (Math.ceil(sbBits / 8.0) * 8);
        int bbBits = MinBits(SUPERBLOCKSIZE);
        bbBits = (int) (Math.ceil(bbBits / 8.0) * 8);

        B1 = new BitPackedUnSignedLongBuffer(Path.of("B" + positions[1].name), null, 0, 1);
        B2 = new BitPackedUnSignedLongBuffer(Path.of("B" + positions[2].name), null, 0, 1);
        B3 = new BitPackedUnSignedLongBuffer(Path.of("B" + positions[3].name), null, 0, 1);

        S1 = new BitPackedUnSignedLongBuffer(Path.of("S" + positions[1].name), null, 0, positions[1].getBitSize(dictWriter));
        S2 = new BitPackedUnSignedLongBuffer(Path.of("S" + positions[2].name), null, 0, positions[2].getBitSize(dictWriter));
        S3 = new BitPackedUnSignedLongBuffer(Path.of("S" + positions[3].name), null, 0, positions[3].getBitSize(dictWriter));

        SB1 = new BitPackedUnSignedLongBuffer(Path.of("SB" + positions[1].name), null, 0, sbBits);
        SB2 = new BitPackedUnSignedLongBuffer(Path.of("SB" + positions[2].name), null, 0, sbBits);
        SB3 = new BitPackedUnSignedLongBuffer(Path.of("SB" + positions[3].name), null, 0, sbBits);

        BB1 = new BitPackedUnSignedLongBuffer(Path.of("BB" + positions[1].name), null, 0, bbBits);
        BB2 = new BitPackedUnSignedLongBuffer(Path.of("BB" + positions[2].name), null, 0, bbBits);
        BB3 = new BitPackedUnSignedLongBuffer(Path.of("BB" + positions[3].name), null, 0, bbBits);

        SB1.writeLong(0); SB2.writeLong(0); SB3.writeLong(0);

        processQuads(dictWriter, allQuads);
        prepareForReading();
    }

    private void processQuads(FiveSectionDictionaryWriter w, Quad[] allQuads) {
        System.out.print("Sorting quads for " + type.name() + "... ");
        Arrays.parallelSort(allQuads, type.getComparator());
        System.out.println("done");

        LevelState l1 = new LevelState();
        LevelState l2 = new LevelState();
        LevelState l3 = new LevelState();

        Quad prev = null;

        for (Quad curr : allQuads) {
            if (prev != null &&
                    positions[0].getNode(prev).equals(positions[0].getNode(curr)) &&
                    positions[1].getNode(prev).equals(positions[1].getNode(curr)) &&
                    positions[2].getNode(prev).equals(positions[2].getNode(curr)) &&
                    positions[3].getNode(prev).equals(positions[3].getNode(curr))) {
                continue;
            }

            if (prev == null) {
                prev = curr;
                continue;
            }

            boolean changeL0 = !positions[0].getNode(prev).equals(positions[0].getNode(curr));
            boolean changeL1 = changeL0 || !positions[1].getNode(prev).equals(positions[1].getNode(curr));
            boolean changeL2 = changeL1 || !positions[2].getNode(prev).equals(positions[2].getNode(curr));
            
            // Level 3
            long id3 = positions[3].locateInDictionary(w, prev);
            S3.writeLong(id3);
            int bit3 = changeL2 ? 1 : 0;
            B3.writeInteger(bit3);
            advanceLevel(l3, bit3, SB3, BB3);

            // Level 2
            if (changeL2) {
                long id2 = positions[2].locateInDictionary(w, prev);
                S2.writeLong(id2);
                int bit2 = changeL1 ? 1 : 0;
                B2.writeInteger(bit2);
                advanceLevel(l2, bit2, SB2, BB2);
            }

            // Level 1
            if (changeL1) {
                long id1 = positions[1].locateInDictionary(w, prev);
                S1.writeLong(id1);
                int bit1 = changeL0 ? 1 : 0;
                B1.writeInteger(bit1);
                advanceLevel(l1, bit1, SB1, BB1);
            }

            prev = curr;
        }

        if (prev != null) {
            long id3 = positions[3].locateInDictionary(w, prev);
            S3.writeLong(id3);
            B3.writeInteger(1); 
            advanceLevel(l3, 1, SB3, BB3);

            long id2 = positions[2].locateInDictionary(w, prev);
            S2.writeLong(id2);
            B2.writeInteger(1); 
            advanceLevel(l2, 1, SB2, BB2);

            long id1 = positions[1].locateInDictionary(w, prev);
            S1.writeLong(id1);
            B1.writeInteger(1); 
            advanceLevel(l1, 1, SB1, BB1);
        }
        
        flushAllBuffers();
    }

    private void advanceLevel(LevelState state, int bitValue, BitPackedUnSignedLongBuffer SB, BitPackedUnSignedLongBuffer BB) {
        if (bitValue == 1) {
            state.onesSoFar++;
            state.onesInCurrentSuperblock++;
            state.onesInCurrentBlock++;
        }

        state.bitsProcessed++;

        long currentSuperblock = state.bitsProcessed / SUPERBLOCKSIZE;
        if (currentSuperblock != state.lastSuperblockWritten) {
            SB.writeLong(state.onesSoFar);
            state.lastSuperblockWritten = currentSuperblock;
            state.onesInCurrentSuperblock = 0; 
        }

        long currentBlock = state.bitsProcessed / BLOCKSIZE;
        if (currentBlock != state.lastBlockWritten) {
            BB.writeLong(state.onesInCurrentSuperblock);
            state.lastBlockWritten = currentBlock;
            state.onesInCurrentBlock = 0;
        }
    }    

    private void flushAllBuffers() {
        B1.complete(); B2.complete(); B3.complete();
        S1.complete(); S2.complete(); S3.complete();
        SB1.complete(); SB2.complete(); SB3.complete();
        BB1.complete(); BB2.complete(); BB3.complete();
    }

    private void prepareForReading() {
        B1.prepareForReading(); B2.prepareForReading(); B3.prepareForReading();
        S1.prepareForReading(); S2.prepareForReading(); S3.prepareForReading();
        SB1.prepareForReading(); SB2.prepareForReading(); SB3.prepareForReading();
        BB1.prepareForReading(); BB2.prepareForReading(); BB3.prepareForReading();
    }

    public void Add(WritableGroup hdt) {
        WritableGroup index = hdt.putGroup(type.name());
        S1.Add(index); S2.Add(index); S3.Add(index);
        B1.Add(index); B2.Add(index); B3.Add(index);
        SB1.Add(index); SB2.Add(index); SB3.Add(index);
        BB1.Add(index); BB2.Add(index); BB3.Add(index);
    }
}

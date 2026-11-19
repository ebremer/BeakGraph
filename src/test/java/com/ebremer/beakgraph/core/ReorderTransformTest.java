package com.ebremer.beakgraph.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the GSPO/GPOS-aware join reorder, driven by synthetic VoID stats so the ordering
 * decisions are exact and deterministic.
 */
class ReorderTransformTest {

    private static final Node COMMON = NodeFactory.createURI("http://ex/common"); // 995 triples
    private static final Node RARE   = NodeFactory.createURI("http://ex/rare");   // 5 triples
    private static final Node OBJ    = NodeFactory.createURI("http://ex/obj");
    private static final Node S1     = NodeFactory.createURI("http://ex/s1");

    /** T=1000, Ds=100, Do=500, Dp=2, with a skewed predicate distribution. */
    private static BGReorderTransform skewed() {
        Map<Node, Long> cP = new HashMap<>();
        cP.put(COMMON, 995L);
        cP.put(RARE, 5L);
        return new BGReorderTransform(VoidStats.of(1000, 100, 500, 2, cP));
    }

    @Test
    void selectivePredicatePlacedFirst() {
        Var x = Var.alloc("x"), y = Var.alloc("y"), z = Var.alloc("z");
        BasicPattern bp = new BasicPattern();
        bp.add(Triple.create(x, COMMON, y));   // index 0: common predicate (995)
        bp.add(Triple.create(x, RARE, z));     // index 1: rare predicate (5)

        BasicPattern out = skewed().reorder(bp);
        assertEquals(RARE, out.get(0).getPredicate(), "the rare predicate should be evaluated first");
        assertEquals(COMMON, out.get(1).getPredicate());
    }

    // ---- exact counts (Counts), in a store shaped like the 38-billion-quad OKN store ----

    private static final Node TYPE      = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    private static final Node FACILITY  = NodeFactory.createURI("http://w3id.org/fio/v1/fio#Facility");
    private static final Node INDUSTRY  = NodeFactory.createURI("http://w3id.org/fio/v1/fio#ofIndustry");
    private static final Node LANDFILL  = NodeFactory.createURI("http://w3id.org/fio/v1/naics#NAICS-562212");
    private static final Node CONNECTED = NodeFactory.createURI("http://purl.org/spatialai/spatial/spatial-full#connectedTo");
    private static final Node REGION3   = NodeFactory.createURI("http://stko-kwg.geog.ucsb.edu/lod/ontology/AdministrativeRegion_3");

    /** The OKN store's shape: dividing by its ~8.7 billion distinct objects puts every ?s <p> <o> below one match. */
    private static BGReorderTransform okn(BGReorderTransform.Counts counts) {
        Map<Node, Long> cP = new HashMap<>();
        cP.put(TYPE, 6_413_078_641L);
        cP.put(INDUSTRY, 7_504_626L);
        cP.put(CONNECTED, 152_433_602L);
        return new BGReorderTransform(VoidStats.of(38_281_567_289L, 4_000_000_000L, 8_713_000_895L, 68_868, cP), counts);
    }

    /** The matches each pattern has in the OKN store. */
    private static long oknMatches(Node p, Node o) {
        if (p.equals(TYPE) && FACILITY.equals(o)) return 4_860_757L;
        if (p.equals(INDUSTRY) && LANDFILL.equals(o)) return 14_306L;
        if (p.equals(TYPE) && REGION3.equals(o)) return 30_000L;
        if (p.equals(CONNECTED) && o == null) return 152_433_602L;
        return -1;
    }

    @Test
    void withoutExactCountsTheStoreWideEstimateCannotTellTwoTypedPatternsApart() {
        // The fault the counts fix: dividing by the store's ~8.7 billion distinct objects puts both
        // patterns near one match, so either order costs the same.
        Var f = Var.alloc("f");
        List<Triple> facilities = List.of(Triple.create(f, TYPE, FACILITY)), landfills = List.of(Triple.create(f, INDUSTRY, LANDFILL));
        BGReorderTransform t = okn(null);
        double facilitiesFirst = t.cost(facilities, new int[] {0});
        double landfillsFirst = t.cost(landfills, new int[] {0});
        assertEquals(facilitiesFirst, landfillsFirst, facilitiesFirst * 1e-6, "4.86 M facilities and 14,306 landfills look alike");
    }

    @Test
    void exactCountsPutTheSelectivePatternFirst() {
        Var f = Var.alloc("f");
        BasicPattern bp = new BasicPattern();
        bp.add(Triple.create(f, TYPE, FACILITY));      // 4,860,757 matches
        bp.add(Triple.create(f, INDUSTRY, LANDFILL));  // 14,306 matches
        BasicPattern out = okn(ReorderTransformTest::oknMatches).reorder(bp);
        assertEquals(INDUSTRY, out.get(0).getPredicate(), "the landfills, not every facility, should be read first");
        assertEquals(TYPE, out.get(1).getPredicate());
    }

    @Test
    void anExactCountGovernsOnlyWhileTheSubjectIsUnbound() {
        // fio-spatial-facilities-in-state's core: once ?t is bound by connectedTo, the region
        // check is a lookup per row, not its 30,000 matches - so it goes last, not second.
        Var f = Var.alloc("f"), t = Var.alloc("t");
        BasicPattern bp = new BasicPattern();
        bp.add(Triple.create(f, TYPE, FACILITY));
        bp.add(Triple.create(f, INDUSTRY, LANDFILL));
        bp.add(Triple.create(f, CONNECTED, t));
        bp.add(Triple.create(t, TYPE, REGION3));
        BasicPattern out = okn(ReorderTransformTest::oknMatches).reorder(bp);
        assertEquals(INDUSTRY, out.get(0).getPredicate());
        assertEquals(t, out.get(3).getSubject(), "the region check waits until ?t is bound: " + out);
    }

    @Test
    void aPatternWithNoMatchesGoesFirst() {
        Var f = Var.alloc("f");
        BasicPattern bp = new BasicPattern();
        bp.add(Triple.create(f, TYPE, FACILITY));
        bp.add(Triple.create(f, INDUSTRY, LANDFILL));
        BasicPattern out = okn((p, o) -> p.equals(TYPE) ? 0 : oknMatches(p, o)).reorder(bp);
        assertEquals(TYPE, out.get(0).getPredicate(), "nothing to join: the empty pattern ends the BGP at once");
    }

    @Test
    void theBoundPlaceholderIsNeverCountedAsATerm() {
        // GRAPH ?g's planner hands BeakGraph.BOUND for a variable bound earlier in the block.
        Var f = Var.alloc("f");
        BasicPattern bp = new BasicPattern();
        bp.add(Triple.create(f, CONNECTED, BeakGraph.BOUND));
        bp.add(Triple.create(f, BeakGraph.BOUND, LANDFILL));
        bp.add(Triple.create(f, INDUSTRY, LANDFILL));
        BasicPattern out = okn((p, o) -> {
            assertNotEquals(BeakGraph.BOUND, p, "the placeholder predicate was counted");
            assertNotEquals(BeakGraph.BOUND, o, "the placeholder object was counted");
            return oknMatches(p, o);
        }).reorder(bp);
        assertEquals(3, out.size());
    }

    // ---- fan-out: sockg/biomassCarbohydrate's core, in sockg's figures ----

    private static final String SOCKG = "https://idir.uta.edu/sockg-ontology#";
    private static final Node BIOMASS     = NodeFactory.createURI(SOCKG + "BiomassCarbohydrate");
    private static final Node PARAMETER   = NodeFactory.createURI(SOCKG + "Parameter");
    private static final Node MEASUREMENT = NodeFactory.createURI(SOCKG + "hasMeasurement");
    private static final Node OF          = NodeFactory.createURI(SOCKG + "of");
    private static final Node LABEL       = NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label");

    /**
     * 1,367 biomass records, 156 parameters, 3,180,012 measurements each of one parameter: about
     * 20,000 measurements a parameter. Rows / distinct objects as GPOS gives them.
     */
    private static final BGReorderTransform.Counts SOCKG_COUNTS = new BGReorderTransform.Counts() {
        @Override
        public long matches(Node p, Node o) {
            if (o == null) {
                return p.equals(TYPE) ? 10_000_000L : p.equals(MEASUREMENT) || p.equals(OF) ? 3_180_012L : p.equals(LABEL) ? 2_000_000L : 0;
            }
            if (p.equals(TYPE)) {
                return o.equals(BIOMASS) ? 1_367 : o.equals(PARAMETER) ? 156 : 0;
            }
            return -1;
        }

        @Override
        public long objects(Node p) {
            return p.equals(TYPE) ? 120 : p.equals(MEASUREMENT) ? 3_180_012L : p.equals(OF) ? 156 : p.equals(LABEL) ? 1_000_000L : 0;
        }

        @Override
        public long subjects(Node p) {
            return p.equals(TYPE) ? 10_000_000L : p.equals(MEASUREMENT) ? 1_000_000L : p.equals(OF) ? 3_180_012L : p.equals(LABEL) ? 2_000_000L : 0;
        }

        @Override public long quads()      { return 60_000_000L; }
        @Override public long subjects()   { return 12_000_000L; }
        @Override public long predicates() { return 300; }
    };

    private static List<Triple> biomass() {
        Var f = Var.alloc("f"), m = Var.alloc("m"), param = Var.alloc("param"), label = Var.alloc("label");
        return List.of(
                Triple.create(param, TYPE, PARAMETER),       // 156 matches: the fewest
                Triple.create(param, LABEL, label),
                Triple.create(m, OF, param),                 // ~20,000 a parameter
                Triple.create(f, MEASUREMENT, m),
                Triple.create(f, TYPE, BIOMASS));            // 1,367 matches
    }

    @Test
    void theJoinBeginsWhereItStaysSmallNotAtTheFewestMatches() {
        List<Triple> written = biomass();
        BasicPattern bp = BasicPattern.wrap(written);
        BGReorderTransform t = okn(SOCKG_COUNTS);
        BasicPattern out = t.reorder(bp);
        assertEquals(BIOMASS, out.get(0).getObject(), "begin at the 1,367 biomass records: " + out);
        int measurement = out.getList().indexOf(written.get(3)), of = out.getList().indexOf(written.get(2));
        assertTrue(measurement < of, "reach each measurement from its record, not from its parameter: " + out);
    }

    @Test
    void aBoundObjectFansOutToItsSubjects() {
        // The order the greedy pick of the fewest matches made: 156 parameters, each to ~20,000
        // measurements - 3.18 million rows - before the 1,367 records narrow them.
        List<Triple> written = biomass();
        BGReorderTransform t = okn(SOCKG_COUNTS);
        double fromParameters = t.cost(written, new int[] {0, 1, 2, 3, 4});
        double chosen = t.cost(written, t.plan(written));
        assertTrue(fromParameters > 3_180_012, "the fan-out through ?m sockg:of ?param must be seen: " + fromParameters);
        assertTrue(chosen * 100 < fromParameters, "chosen " + chosen + " vs from the parameters " + fromParameters);
    }

    @Test
    void writtenOrderStaysWhenNothingIsCheaper() {
        List<Triple> written = List.of(biomass().get(4), biomass().get(3), biomass().get(2), biomass().get(0), biomass().get(1));
        BGReorderTransform t = okn(SOCKG_COUNTS);
        int[] order = t.plan(written);
        assertTrue(t.cost(written, order) <= t.cost(written, new int[] {0, 1, 2, 3, 4}));
        assertEquals(0, order[0]);
    }

    // ---- fan-out from a bound subject: ubergraph/abdominal-cell-types, in ubergraph's figures ----

    private static final String OBO = "http://purl.obolibrary.org/obo/";
    private static final Node SUBCLASS = NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#subClassOf");
    private static final Node PART_OF  = NodeFactory.createURI(OBO + "BFO_0000050");
    private static final Node CELL     = NodeFactory.createURI(OBO + "CL_0000000");
    private static final Node ORGAN    = NodeFactory.createURI(OBO + "UBERON_0000062");
    private static final Node ABDOMEN  = NodeFactory.createURI(OBO + "UBERON_0000916");

    /**
     * As counted in ubergraph: 31,721 cell types; part_of has 4,735,108 rows over 19,824 objects
     * and 125,961 subjects - 38 a subject, 66 a cell type. Divided by all 17,382,645 subjects in
     * the graph, it looked like 1. 838 parts of the abdomen; 4,593 organs; 3,886,036 subjects of
     * rdfs:subClassOf.
     */
    private static BGReorderTransform.Counts ubergraph(boolean subjectsOfPartOf) {
        return new BGReorderTransform.Counts() {
            @Override
            public long matches(Node p, Node o) {
                if (o == null) {
                    return p.equals(SUBCLASS) ? 30_000_000L : p.equals(PART_OF) ? 4_735_108L : p.equals(LABEL) ? 5_000_000L : 0;
                }
                if (p.equals(SUBCLASS)) return o.equals(CELL) ? 31_721 : o.equals(ORGAN) ? 4_593 : 0;
                if (p.equals(PART_OF)) return o.equals(ABDOMEN) ? 838 : 0;
                return -1;
            }

            @Override
            public long objects(Node p) {
                return p.equals(SUBCLASS) ? 1_000_000L : p.equals(PART_OF) ? 19_824 : p.equals(LABEL) ? 4_900_000L : 0;
            }

            @Override
            public long subjects(Node p) {
                if (p.equals(PART_OF)) return subjectsOfPartOf ? 125_961 : -1;
                return p.equals(SUBCLASS) ? 3_886_036L : p.equals(LABEL) ? 5_000_000L : 0;
            }

            @Override public long quads()      { return 120_000_000L; }
            @Override public long subjects()   { return 17_382_645L; }
            @Override public long predicates() { return 2_000; }
        };
    }

    private static List<Triple> abdominalCells() {
        Var cell = Var.alloc("cell"), organ = Var.alloc("organ"), cl = Var.alloc("cl"), ol = Var.alloc("ol");
        return List.of(
                Triple.create(cell, SUBCLASS, CELL),          // 31,721 cell types
                Triple.create(cell, PART_OF, organ),          // 66 a cell type
                Triple.create(organ, SUBCLASS, ORGAN),
                Triple.create(organ, PART_OF, ABDOMEN),       // 838 parts of the abdomen
                Triple.create(cell, LABEL, cl),
                Triple.create(organ, LABEL, ol));
    }

    @Test
    void aBoundSubjectFansOutToItsObjects() {
        List<Triple> written = abdominalCells();
        BasicPattern out = okn(ubergraph(true)).reorder(BasicPattern.wrap(written));
        assertEquals(ABDOMEN, out.get(0).getObject(), "begin at the 838 parts of the abdomen, not the 31,721 cells: " + out);
    }

    @Test
    void withoutThePredicatesSubjectsACellLooksPartOfOneThing() {
        // Rows ÷ the graph's subjects: 4.7 M ÷ 17.4 M, under one - every cell's parts looked cheap.
        List<Triple> written = abdominalCells();
        int[] cellsFirst = {0, 1, 2, 3, 4, 5};
        double seen = okn(ubergraph(true)).cost(written, cellsFirst);
        double unseen = okn(ubergraph(false)).cost(written, cellsFirst);
        assertTrue(seen > 10 * unseen, "the cells' 2 million parts: " + seen + " vs " + unseen);
    }

    // ---- a cross product waits: federation/sawgraph-hydrology-spatial_03's core ----

    private static final Node WELL_TYPE = NodeFactory.createURI("http://sawgraph.spatialai.org/v1/me-mgs#ofWellType");
    private static final Node GRAVEL    = NodeFactory.createURI("http://sawgraph.spatialai.org/v1/me-mgs-data#d.wellType.Gravel");
    private static final Node SF_WITHIN = NodeFactory.createURI("http://stko-kwg.geog.ucsb.edu/lod/ontology/sfWithin");
    private static final Node S2_CELL   = NodeFactory.createURI("http://stko-kwg.geog.ucsb.edu/lod/ontology/S2Cell_Level13");
    private static final Node AQ_TYPE   = NodeFactory.createURI("http://sawgraph.spatialai.org/v1/saw_water#aquiferType");
    private static final Node SAND      = NodeFactory.createLiteralString("sand and gravel");

    /** 5,000 gravel wells, each sfWithin ~150 cells and regions; 100 sand-and-gravel aquifers. */
    private static final BGReorderTransform.Counts WELLS = new BGReorderTransform.Counts() {
        @Override
        public long matches(Node p, Node o) {
            if (o == null) {
                return p.equals(WELL_TYPE) ? 50_000 : p.equals(SF_WITHIN) ? 300_000_000L : p.equals(AQ_TYPE) ? 1_000 : p.equals(TYPE) ? 6_000_000_000L : 0;
            }
            if (p.equals(WELL_TYPE)) return o.equals(GRAVEL) ? 5_000 : 0;
            if (p.equals(AQ_TYPE)) return o.equals(SAND) ? 100 : 0;
            if (p.equals(TYPE)) return o.equals(S2_CELL) ? 10_000_000L : 0;
            return -1;
        }

        @Override
        public long objects(Node p) {
            return p.equals(WELL_TYPE) ? 10 : p.equals(SF_WITHIN) ? 5_000_000L : p.equals(AQ_TYPE) ? 5 : p.equals(TYPE) ? 100_000 : 0;
        }

        @Override
        public long subjects(Node p) {
            return p.equals(WELL_TYPE) ? 50_000 : p.equals(SF_WITHIN) ? 2_000_000L : p.equals(AQ_TYPE) ? 1_000 : p.equals(TYPE) ? 3_000_000_000L : 0;
        }
    };

    @Test
    void aCrossProductWaitsUntilNothingJoinedIsLeft() {
        // From the wells, the aquifers (100 rows, sharing no variable) looked cheaper than a well's
        // ~150 cells, and ran second: every later step ran on wells x aquifers.
        Var well = Var.alloc("well"), cell = Var.alloc("cell"), aq = Var.alloc("aq");
        List<Triple> written = List.of(
                Triple.create(well, WELL_TYPE, GRAVEL),
                Triple.create(aq, AQ_TYPE, SAND),
                Triple.create(well, SF_WITHIN, cell),
                Triple.create(cell, TYPE, S2_CELL));
        int[] order = okn(WELLS).orderFrom(written, 0);
        assertEquals(1, order[3], "the aquifers last: " + java.util.Arrays.toString(order));
    }

    @Test
    void aCountyCheckNarrowsTheWellsBeforeTheAquifers() {
        // The query's start: the 5,000 wells, each within ~150 cells, of which the county's 2,000
        // (of 5 million) keep few; then the 100 aquifers. Halving at the county check, beginning at
        // the aquifers and multiplying all the rest by them looked as cheap.
        Var well = Var.alloc("well"), cell = Var.alloc("cell"), aq = Var.alloc("aq");
        Node county = NodeFactory.createURI("http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.23019");
        Node aquifer = NodeFactory.createURI("http://gwml2.org/def/gwml2#GW_Aquifer");
        List<Triple> written = List.of(
                Triple.create(aq, AQ_TYPE, SAND),
                Triple.create(aq, TYPE, aquifer),
                Triple.create(well, WELL_TYPE, GRAVEL),
                Triple.create(well, SF_WITHIN, cell),
                Triple.create(cell, CONNECTED, county));
        BGReorderTransform t = okn(new BGReorderTransform.Counts() {
            @Override
            public long matches(Node p, Node o) {
                if (p.equals(CONNECTED)) return (o == null) ? 152_433_602L : o.equals(county) ? 2_000 : 0;
                if (p.equals(TYPE) && aquifer.equals(o)) return 1_000;
                return WELLS.matches(p, o);
            }
            @Override public long objects(Node p) { return p.equals(CONNECTED) ? 10_000_000L : WELLS.objects(p); }
            @Override public long subjects(Node p) { return p.equals(CONNECTED) ? 5_000_000L : WELLS.subjects(p); }
        });
        BasicPattern out = t.reorder(BasicPattern.wrap(written));
        assertEquals(Set.of(written.get(0), written.get(1)), Set.of(out.get(3), out.get(4)), "the aquifers last: " + out);
    }

    @Test
    void aFullyBoundCheckGoesFirst() {
        // The query's second block, per row of the first: is the well's cell connected to the
        // aquifer? For most rows of wells x aquifers it is not, and asking first ends the row
        // before the aquifer's cells are listed.
        Var sp = Var.alloc("sp");
        Node aquifer = NodeFactory.createURI("http://sawgraph.spatialai.org/v1/me_mgs_data#d.MGS-Aquifer.00005");
        Node wellCell = NodeFactory.createURI("https://datacommons.org/browser/geoId/2301937760");
        Node gwAquifer = NodeFactory.createURI("http://gwml2.org/def/gwml2#GW_Aquifer");
        List<Triple> written = List.of(
                Triple.create(sp, CONNECTED, aquifer),
                Triple.create(sp, TYPE, S2_CELL),
                Triple.create(wellCell, CONNECTED, aquifer),
                Triple.create(aquifer, TYPE, gwAquifer),
                Triple.create(wellCell, TYPE, S2_CELL));
        BGReorderTransform t = okn(new BGReorderTransform.Counts() {
            @Override
            public long matches(Node p, Node o) {
                if (p.equals(CONNECTED)) return (o == null) ? 152_433_602L : o.equals(aquifer) ? 8 : 0;
                return WELLS.matches(p, o);
            }
            @Override public long objects(Node p) { return p.equals(CONNECTED) ? 10_000_000L : WELLS.objects(p); }
            @Override public long subjects(Node p) { return p.equals(CONNECTED) ? 5_000_000L : WELLS.subjects(p); }
        });
        BasicPattern out = t.reorder(BasicPattern.wrap(written));
        assertEquals(written.get(2), out.get(0), "the cell's connection first: " + out);
    }

    @Test
    void aTermLookupIsNoCrossProduct() {
        // At most a row a row - a term's label - it need not wait.
        Var well = Var.alloc("well"), cell = Var.alloc("cell"), name = Var.alloc("name");
        Node county = NodeFactory.createURI("http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.23019");
        List<Triple> written = List.of(
                Triple.create(well, WELL_TYPE, GRAVEL),
                Triple.create(well, SF_WITHIN, cell),
                Triple.create(county, LABEL, name));
        BGReorderTransform t = okn(new BGReorderTransform.Counts() {
            @Override public long matches(Node p, Node o) { return !p.equals(LABEL) ? WELLS.matches(p, o) : (o == null) ? 5_000_000L : -1; }
            @Override public long objects(Node p) { return p.equals(LABEL) ? 5_000_000L : WELLS.objects(p); }
            @Override public long subjects(Node p) { return p.equals(LABEL) ? 5_000_000L : WELLS.subjects(p); }
        });
        int[] order = t.orderFrom(written, 0);
        assertEquals(2, order[1], "the county's label before the wells' cells: " + java.util.Arrays.toString(order));
    }

    @Test
    void objectOnlyPatternDeferredUntilJoinable() {
        Var a = Var.alloc("a"), b = Var.alloc("b");
        BasicPattern bp = new BasicPattern();
        bp.add(Triple.create(a, b, OBJ));       // index 0: object-only -> no GSPO/GPOS prefix
        bp.add(Triple.create(S1, RARE, a));     // index 1: subject + predicate bound -> index prefix

        BasicPattern out = skewed().reorder(bp);
        // The indexable triple must run first; the object-only one is deferred until ?a is bound.
        assertEquals(S1, out.get(0).getSubject(), "the indexable triple should run first");
        assertEquals(OBJ, out.get(1).getObject(), "the object-only triple should be deferred");
    }
}

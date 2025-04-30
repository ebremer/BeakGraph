package com.ebremer.beakgraph.hdtish;

import java.util.Comparator;
import org.apache.jena.graph.Node;

public class NodeComparator implements Comparator<Node> {
    @Override
    public int compare(Node n1, Node n2) {
        // Step 1: Compare node types
        int type1 = getType(n1);
        int type2 = getType(n2);
        if (type1 != type2) {
            return Integer.compare(type1, type2); // Blank < URI < Literal
        }

        // Step 2: Compare within the same type
        if (n1.isBlank() && n2.isBlank()) {
            // Blank nodes: Compare by internal label (implementation-dependent)
            return n1.getBlankNodeLabel().compareTo(n2.getBlankNodeLabel());
        } else if (n1.isURI() && n2.isURI()) {
            // URIs: Compare by string value
            return n1.getURI().compareTo(n2.getURI());
        } else if (n1.isLiteral() && n2.isLiteral()) {
            // Literals: Compare by datatype, language tag, and lexical form
            String dt1 = n1.getLiteralDatatypeURI();
            String dt2 = n2.getLiteralDatatypeURI();
            String lang1 = n1.getLiteralLanguage();
            String lang2 = n2.getLiteralLanguage();
            String lex1 = n1.getLiteralLexicalForm();
            String lex2 = n2.getLiteralLexicalForm();

            // Handle literals with language tags
            if (!lang1.isEmpty() && !lang2.isEmpty()) {
                int langComp = lang1.compareTo(lang2);
                return langComp != 0 ? langComp : lex1.compareTo(lex2);
            } else if (!lang1.isEmpty()) {
                return 1; // n2 (no lang tag) comes first
            } else if (!lang2.isEmpty()) {
                return -1; // n1 (no lang tag) comes first
            } else {
                // Handle plain literals and typed literals
                if (dt1 == null && dt2 == null) {
                    return lex1.compareTo(lex2); // Both plain literals
                } else if (dt1 == null) {
                    return -1; // Plain literals before typed literals
                } else if (dt2 == null) {
                    return 1;
                } else {
                    int dtComp = dt1.compareTo(dt2);
                    return dtComp != 0 ? dtComp : lex1.compareTo(lex2);
                }
            }
        }
        return 0; // Equal if all conditions match
    }

    // Assign numeric values to node types for ordering
    private int getType(Node n) {
        if (n.isBlank()) return 0;
        if (n.isURI()) return 1;
        if (n.isLiteral()) return 2;
        throw new IllegalArgumentException("Unknown node type");
    }
}
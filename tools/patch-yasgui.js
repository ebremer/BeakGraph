// Patches the bundled YASGUI 2.0.12 (src/main/resources/META-INF/sparql/yasgui.min.js), the endpoint's web
// page at /sparql/, so its results viewer shows rdf-array sequence terms: {"type": "seq", "value": [...]} in
// SPARQL JSON results, with {"type": "undef"} for an empty slot. Unpatched, the viewer's {{var}} substitution
// called .replace on a sequence's list, threw, and left the results pane empty.
//
// The bundle in the repository is already patched. Run this again only after replacing yasgui.min.js with a
// fresh copy. Every edit must find its text exactly once, so a second run, or a YASGUI whose code moved,
// stops with an error and leaves the file unchanged:
//   node tools/patch-yasgui.js src/main/resources/META-INF/sparql/yasgui.min.js
// The edits shift columns on the bundle's long lines, so yasgui.min.js.map is slightly off after them. The
// map is only used for debugging.
// The editor still marks UNNEST and other SPARQL-array syntax as errors: its grammar is SPARQL 1.1. Queries
// run anyway.
const fs = require("fs");
const path = process.argv[2];
let s = fs.readFileSync(path, "utf8");

// A term as text, Turtle-like: ( 60 120 200 ), numbers and booleans bare, strings quoted, IRIs in <>.
const TEXT = 'function bgSeqText(e){var d=e.datatype,x="http://www.w3.org/2001/XMLSchema#";'
    + 'return"seq"==e.type?"( "+e.value.map(bgSeqText).join(" ")+" )"'
    + ':"undef"==e.type?"undef"'
    + ':"triple"==e.type?"<<( "+[e.value.subject,e.value.predicate,e.value.object].map(bgSeqText).join(" ")+" )>>"'
    + ':"uri"==e.type?"<"+e.value+">"'
    + ':"bnode"==e.type?"_:"+e.value'
    + ':"string"!=typeof e.value?String(e.value)'
    + ':e["xml:lang"]?JSON.stringify(e.value)+"@"+e["xml:lang"]'
    + ':d&&[x+"integer",x+"decimal",x+"double",x+"boolean"].indexOf(d)>=0?e.value'
    + ':d&&d!=x+"string"?JSON.stringify(e.value)+"^^<"+d+">"'
    + ':JSON.stringify(e.value)}';
const text = (arg) => "(" + TEXT + ")(" + arg + ")";

const edits = [
    // 1. The parser's {{var}} substitution called .replace on every non-IRI value: a sequence's value is a
    //    list, so it threw and the results pane stayed empty.
    ['"uri"!==e[t].type&&(e[t].value=e[t].value.replace(',
     '"uri"!==e[t].type&&"string"==typeof e[t].value&&(e[t].value=e[t].value.replace('],
    // 2. The table cell: a sequence as ( a b c ), its IRIs as links like any other, nested sequences inside.
    ['{key:"getCellContent",value:function(e,t){var r;return r="uri"==e.type?',
     '{key:"formatSeq",value:function(e,t){var r=this;return"seq"==e.type?"( "+e.value.map((function(e){return r.formatSeq(e,t)})).join(" ")+" )"'
        + ':"uri"==e.type?r.getUriLinkFromBinding(e,t):(0,bt.A)(' + text("e") + ')}},'
        + '{key:"getCellContent",value:function(e,t){var r;return r="seq"==e.type?"<span class=\'nonIri\'>".concat(ht(this.formatSeq(e,t)),"</span>"):"uri"==e.type?'],
    // 3. The table's sort and search text.
    ['t.getCellContent(e,r):ht(e.value)}',
     't.getCellContent(e,r):ht("seq"==e.type?(0,bt.A)(' + text("e") + '):e.value)}'],
    // 4. The CSV download.
    ['return(0,T.A)(e,(function(e){return e.value}))',
     'return(0,T.A)(e,(function(e){return"seq"==e.type?' + text("e") + ':e.value}))'],
];

for (const [from, to] of edits) {
    const n = s.split(from).length - 1;
    if (n !== 1) { console.error("expected one occurrence, found " + n + ": " + from); process.exit(1); }
    s = s.replace(from, () => to);
}
fs.writeFileSync(path, s);
console.log("patched " + edits.length + " places in " + path);

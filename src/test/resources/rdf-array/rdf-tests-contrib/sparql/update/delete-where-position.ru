PREFIX : <http://example/>
DELETE { ?s :tags ?seq } INSERT { ?s :tag ?t } WHERE { ?s :tags ?seq UNNEST(?seq AS ?t) }

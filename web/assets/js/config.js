/**
 * Application Configuration
 */

export const config = {
    // Database paths (relative to web root)
    databases: {
        corpus: 'assets/data/corpus.duckdb',
        lexicon: 'assets/data/lexicon.duckdb'
    },

    // Database versions (for cache invalidation)
    versions: {
        corpus: '1.0.0',
        lexicon: '1.0.0'
    },

    // Database sizes (in bytes) for progress calculation
    sizes: {
        corpus: 741 * 1024 * 1024,  // 741 MB
        lexicon: 84 * 1024 * 1024   // 84 MB
    },

    // External resource URL patterns
    externalLinks: {
        tla: (tlaId) => `https://thesaurus-linguae-aegyptiae.de/lemma/${tlaId}`,
        oraec: (oraecId) => `https://oraec.github.io/corpus/search.xql?query=${oraecId}`,
        cdo: (cdoId) => `https://coptic-dictionary.org/entry.cgi?tla=${cdoId}`,
        scriptorium: (docId) => `https://data.copticscriptorium.org/${docId}`,
        hieroglyph: (unicode) => `https://unicode-table.com/en/${unicode.codePointAt(0).toString(16).toUpperCase()}/`
    },

    // UI configuration
    ui: {
        maxResultsDefault: 100,
        concordanceContextWidth: 40,
        autocompleteLimit: 10
    }
};

export default config;

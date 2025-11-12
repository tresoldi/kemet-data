/**
 * Configuration Example for External Database Hosting
 *
 * Copy this to assets/js/config.js and update the database URLs
 * to point to your externally hosted database files.
 */

export const config = {
    // IMPORTANT: Update these URLs to point to your hosted database files
    // The databases are too large for git (741 MB + 84 MB)
    //
    // Recommended hosting options:
    // - Zenodo: https://zenodo.org (free, for research data)
    // - GitHub Releases: Attach to a release in your repository
    // - Google Drive: Generate public shareable links
    // - AWS S3 / DigitalOcean Spaces: Object storage
    //
    // See DEPLOYMENT.md for detailed instructions
    databases: {
        // Example URLs (replace with your own):

        // Option 1: Zenodo
        // corpus: 'https://zenodo.org/records/XXXXX/files/corpus.duckdb',
        // lexicon: 'https://zenodo.org/records/XXXXX/files/lexicon.duckdb'

        // Option 2: GitHub Releases
        // corpus: 'https://github.com/tresoldi/kemet-data/releases/download/v1.0/corpus.duckdb',
        // lexicon: 'https://github.com/tresoldi/kemet-data/releases/download/v1.0/lexicon.duckdb'

        // Option 3: Custom CDN/hosting
        // corpus: 'https://your-cdn.example.com/corpus.duckdb',
        // lexicon: 'https://your-cdn.example.com/lexicon.duckdb'

        // Default: Local files (for development only)
        corpus: 'assets/data/corpus.duckdb',
        lexicon: 'assets/data/lexicon.duckdb'
    },

    // Database versions (for cache invalidation)
    // Increment these when you upload new database versions
    versions: {
        corpus: '1.0.0',
        lexicon: '1.0.0'
    },

    // Database sizes (in bytes) for progress calculation
    // Update these if your database sizes change
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

# KEMET Data Explorer - Web Interface

An interactive, client-side web interface for exploring the KEMET Data corpus and lexicon databases of Ancient Egyptian and Coptic texts.

## Features

### 🔍 Dictionary Search
- **Separate search boxes** for Coptic, Egyptian (transliteration), and Greek
- **Real-time autocomplete** with keyboard navigation (arrow keys, Enter, Escape)
- **Comprehensive lemma cards** displaying:
  - Glosses in multiple languages (English, German)
  - Hieroglyphic writing (for Egyptian)
  - Dialectal forms (Sahidic, Bohairic)
  - Morphological forms with frequencies
  - Distribution across collections and periods
  - Etymology (for Coptic lemmas)
  - Example concordances

### 📊 Query Builder (Intermediate Users)
- **6 parameterized query templates**:
  1. **Concordance (KWIC)** - Find lemma occurrences with context
  2. **Frequency Comparison** - Compare lemmas across collections/periods
  3. **Form Variation** - Analyze morphological variations
  4. **Attestation Timeline** - Track usage across historical periods
  5. **Gloss Search** - Find lemmas by translation
  6. **Co-occurrence Analysis** - Find frequently co-occurring lemmas
- **Dynamic form generation** with validation
- **SQL preview** before execution
- **Results export** to CSV/JSON

### 💻 SQL Editor (Advanced Users)
- **Full SQL access** to both databases
- **18 cookbook queries** across 4 categories:
  - Corpus queries (statistics, concordances, POS distribution)
  - Lexicon queries (dictionary lookup, frequency lists, morphology)
  - Etymology & cross-references (CDO, TLA, ORAEC)
  - Advanced queries (semantic search, cross-database joins)
- **Ctrl+Enter** to execute queries
- **Basic SQL formatting**
- **Schema quick reference**
- **Sortable results** with export

### 📤 Export Capabilities
- Export all query results to **CSV** or **JSON**
- Properly escaped CSV with UTF-8 support
- Formatted JSON with indentation

## Technical Architecture

### Client-Side Only
- **No server required** - runs entirely in the browser
- **Static files** deployable to GitHub Pages, Netlify, etc.
- **Offline-capable** after initial database download

### Database Technology
- **DuckDB-WASM** - Full SQL database engine in the browser
- **IndexedDB caching** - Databases cached locally using localForage
- **Progressive download** with real-time progress tracking
- **One-time 825 MB download** (741 MB corpus + 84 MB lexicon)
- **Instant loading** on subsequent visits

### No Build Tools Required
- **Vanilla JavaScript** ES2020 modules
- **No npm/webpack/bundler** needed
- **CDN dependencies** only:
  - DuckDB-WASM 1.28.0
  - localForage 1.10.0

### Browser Support
- **Chrome/Edge** 90+
- **Firefox** 88+
- **Safari** 14+

Requirements:
- ES2020 modules support
- WebAssembly support
- IndexedDB support
- Fetch API with streams

## File Structure

```
web/
├── index.html                      # Main HTML shell
├── README.md                       # This file
├── IMPLEMENTATION_STATUS.md        # Development progress
├── assets/
│   ├── css/
│   │   └── main.css                # Complete styling
│   ├── js/
│   │   ├── config.js               # Configuration
│   │   ├── db-manager.js           # Database handling
│   │   ├── app.js                  # Main application & routing
│   │   ├── components/
│   │   │   ├── loading-screen.js   # Database download UI
│   │   │   ├── dictionary.js       # Dictionary search
│   │   │   ├── lemma-detail.js     # Lemma cards
│   │   │   ├── concordance-view.js # KWIC display
│   │   │   ├── query-builder.js    # Parameterized queries
│   │   │   ├── sql-editor.js       # SQL interface
│   │   │   └── results-table.js    # Sortable tables
│   │   └── utils/
│   │       ├── unicode-utils.js    # Script detection
│   │       ├── export.js           # CSV/JSON export
│   │       ├── sql-templates.js    # Query templates
│   │       └── cookbook.js         # Example queries
│   └── data/
│       ├── corpus.duckdb           # To be copied here
│       └── lexicon.duckdb          # To be copied here
```

## Setup for Development

### 1. Copy Databases

From the project root:

```bash
# Create data directory
mkdir -p web/assets/data

# Copy databases
cp data/derived/corpus.duckdb web/assets/data/
cp data/derived/lexicon.duckdb web/assets/data/
```

### 2. Start Local Web Server

The interface requires a web server (cannot use `file://` protocol due to CORS and ES modules).

**Option A: Python**
```bash
cd web/
python -m http.server 8000
# Open http://localhost:8000
```

**Option B: Node.js**
```bash
cd web/
npx http-server -p 8000
# Open http://localhost:8000
```

**Option C: VS Code Live Server**
- Install "Live Server" extension
- Right-click `index.html` → "Open with Live Server"

### 3. First Load

On first visit:
1. Loading screen appears
2. Databases download (825 MB total)
3. Progress bars show download status
4. Databases cached in IndexedDB
5. Interface becomes available

**Subsequent visits:** Instant loading from cache!

## Deployment to GitHub Pages

### Option 1: Deploy `web/` Directory

```bash
# From project root
cp -r web/ docs/
cp data/derived/*.duckdb docs/assets/data/

# Commit and push
git add docs/
git commit -m "Deploy web interface to GitHub Pages"
git push

# Enable GitHub Pages in repository settings:
# Settings → Pages → Source: docs/ directory
```

**Warning:** GitHub has file size limits:
- Maximum 100 MB per file
- Repository should be < 1 GB

### Option 2: Use Git LFS

```bash
# Install Git LFS
git lfs install

# Track database files
git lfs track "docs/assets/data/*.duckdb"

# Add and commit
git add .gitattributes docs/
git commit -m "Deploy with Git LFS"
git push
```

### Option 3: External Database Hosting

Host databases separately (Zenodo, Figshare, etc.) and update `web/assets/js/config.js`:

```javascript
databases: {
    corpus: 'https://zenodo.org/record/XXXXX/files/corpus.duckdb',
    lexicon: 'https://zenodo.org/record/XXXXX/files/lexicon.duckdb'
}
```

## Configuration

Edit `web/assets/js/config.js` to customize:

```javascript
export const config = {
    // Database URLs
    databases: {
        corpus: 'assets/data/corpus.duckdb',
        lexicon: 'assets/data/lexicon.duckdb'
    },

    // Version numbers (for cache invalidation)
    versions: {
        corpus: '1.0.0',
        lexicon: '1.0.0'
    },

    // UI settings
    ui: {
        maxResultsDefault: 100,
        concordanceContextWidth: 40,
        autocompleteLimit: 10
    },

    // External links
    externalLinks: {
        tla: (tlaId) => `https://thesaurus-linguae-aegyptiae.de/lemma/${tlaId}`,
        oraec: (oraecId) => `https://oraec.github.io/corpus/search.xql?query=${oraecId}`,
        cdo: (cdoId) => `https://coptic-dictionary.org/entry.cgi?tla=${cdoId}`,
        // ...
    }
};
```

## Usage Examples

### Dictionary Search

1. Navigate to **Dictionary** tab
2. Type in Coptic, Egyptian, or Greek search box
3. Select lemma from autocomplete dropdown
4. View comprehensive lemma card with:
   - Forms and morphology
   - Distribution statistics
   - Etymology (if available)
   - Example contexts

### Query Builder

1. Navigate to **Query Builder** tab
2. Select a query template (e.g., "Concordance")
3. Fill in parameters (lemma ID, filters, limits)
4. Click "Generate SQL" to preview
5. Click "Execute Query" to run
6. Export results to CSV/JSON if needed

### SQL Editor

1. Navigate to **SQL Editor** tab
2. Either:
   - Click a cookbook example to load
   - Write custom SQL query
3. Press Ctrl+Enter (or click "Execute")
4. View results in sortable table
5. Export to CSV/JSON

## Performance Notes

### Database Size
- **Corpus:** 741 MB (1.5M tokens)
- **Lexicon:** 84 MB (33K lemmas)
- **Total download:** 825 MB (one-time)

### Query Performance
- **Simple queries:** < 100ms
- **Complex joins:** 100ms - 1s
- **Full table scans:** 1s - 5s

### Caching
- Databases cached in IndexedDB
- Versioned cache (updates on version change)
- No network requests after initial download

## Troubleshooting

### Database Won't Load
- Check browser console for errors
- Ensure DuckDB-WASM CDN is accessible
- Try clearing IndexedDB: Developer Tools → Application → IndexedDB → Delete

### Slow Queries
- Add indexes if modifying database schema
- Use LIMIT clauses for large result sets
- Check query plan with EXPLAIN

### Out of Memory
- Close other browser tabs
- Restart browser
- Use simpler queries with LIMIT

### CDN Issues
- If CDN is down, download DuckDB-WASM locally
- Update script tags in `index.html` to point to local files

## Development Notes

### Adding New Query Templates

Edit `web/assets/js/utils/sql-templates.js`:

```javascript
export const queryTemplates = {
    myNewQuery: {
        id: 'my_new_query',
        name: 'My Query Name',
        description: 'Query description',
        category: 'lexicon', // or 'corpus'
        parameters: [
            {
                name: 'param_name',
                label: 'Parameter Label',
                type: 'text', // text, number, select, checkbox, lemma_search
                required: true,
                placeholder: 'Example value'
            }
        ],
        buildSQL: (params) => {
            return `SELECT * FROM lexicon.lemmas WHERE lemma = '${params.param_name}'`;
        }
    }
};
```

### Adding Cookbook Examples

Edit `web/assets/js/utils/cookbook.js`:

```javascript
export const cookbookQueries = {
    myExample: {
        id: 'my_example',
        name: 'Example Name',
        description: 'What this query does',
        category: 'corpus', // corpus, lexicon, etymology, advanced
        database: 'corpus', // corpus, lexicon, or 'corpus+lexicon'
        sql: `SELECT * FROM corpus.documents LIMIT 10;`
    }
};
```

### Modifying Styles

Edit `web/assets/css/main.css`:
- CSS variables in `:root` for theming
- Responsive breakpoints at 768px
- Special classes for Coptic/hieroglyphs/transliteration

## License

The KEMET Data Explorer web interface is part of the KEMET Data project and is released under **CC BY-SA 4.0**.

Individual data sources retain their original licenses:
- ORAEC: CC0
- TLA: CC BY-SA 4.0
- CDO: CC BY-SA 4.0
- Coptic Scriptorium: CC BY 4.0

## Links

- **Project Repository:** https://github.com/tresoldi/kemet-data
- **Database Documentation:** [DATABASE.md](../DATABASE.md)
- **TLA:** https://thesaurus-linguae-aegyptiae.de/
- **CDO:** https://coptic-dictionary.org/
- **ORAEC:** https://oraec.github.io/
- **Coptic Scriptorium:** https://copticscriptorium.org/

## Citation

When citing this resource:

```
KEMET Data: Integrated Ancient Egyptian and Coptic Linguistic Database
Authors: Tiago Tresoldi and Marwan Kilani
Web Interface v1.0
Available at: https://github.com/tresoldi/kemet-data
```

## Support

For bug reports, feature requests, or questions:
- Open an issue on GitHub
- Check existing documentation in DATABASE.md
- Review example queries in the cookbook

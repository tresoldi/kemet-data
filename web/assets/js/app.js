/**
 * Main Application
 * Handles routing, initialization, and view management
 */

import dbManager from './db-manager.js';
import config from './config.js';

class App {
    constructor() {
        this.currentView = null;
        this.dbReady = false;
        this.views = {
            'dictionary': null,
            'query-builder': null,
            'sql-editor': null,
            'about': null
        };
    }

    async initialize() {
        console.log('Initializing KEMET Data Explorer...');

        // Set up database event listeners
        dbManager.addEventListener('progress', (e) => {
            this.handleDatabaseProgress(e.detail);
        });

        dbManager.addEventListener('ready', () => {
            this.handleDatabaseReady();
        });

        dbManager.addEventListener('error', (e) => {
            this.handleDatabaseError(e.detail);
        });

        // Set up routing
        window.addEventListener('hashchange', () => this.handleRoute());

        // Set up navigation
        this.setupNavigation();

        // Initialize database (will trigger loading screen)
        try {
            await dbManager.initialize();
        } catch (error) {
            console.error('Database initialization failed:', error);
            this.showError('Failed to initialize databases. Please refresh the page.');
        }

        // Handle initial route
        this.handleRoute();
    }

    setupNavigation() {
        const navLinks = document.querySelectorAll('.main-nav a');
        navLinks.forEach(link => {
            link.addEventListener('click', (e) => {
                // Remove active class from all links
                navLinks.forEach(l => l.classList.remove('active'));
                // Add active class to clicked link
                e.target.classList.add('active');
            });
        });
    }

    handleDatabaseProgress(detail) {
        const event = new CustomEvent('db-progress', { detail });
        window.dispatchEvent(event);
    }

    handleDatabaseReady() {
        console.log('Databases ready!');
        this.dbReady = true;

        // Hide loading screen
        const loadingOverlay = document.getElementById('loading-overlay');
        if (loadingOverlay) {
            loadingOverlay.style.display = 'none';
        }

        // Show main app
        const appContainer = document.getElementById('app');
        if (appContainer) {
            appContainer.style.display = 'block';
        }

        // Dispatch ready event
        window.dispatchEvent(new CustomEvent('db-ready'));

        // Load current view if not already loaded
        if (!this.currentView) {
            this.handleRoute();
        }
    }

    handleDatabaseError(error) {
        console.error('Database error:', error);
        this.showError(`Database error: ${error.message || 'Unknown error'}`);
    }

    handleRoute() {
        const hash = window.location.hash.slice(1) || 'dictionary';
        const [view, ...params] = hash.split('/');

        // If database not ready, wait
        if (!this.dbReady && view !== 'about') {
            console.log(`Waiting for database to load ${view} view...`);
            return;
        }

        this.loadView(view, params);
    }

    async loadView(viewName, params = []) {
        // Hide all view containers
        const containers = document.querySelectorAll('.view-container');
        containers.forEach(c => c.classList.remove('active'));

        // Show the requested view container
        const container = document.getElementById(`${viewName}-view`);
        if (!container) {
            console.error(`View container not found: ${viewName}-view`);
            this.showError(`Page not found: ${viewName}`);
            return;
        }

        container.classList.add('active');
        this.currentView = viewName;

        // Load view component if not already loaded
        if (!this.views[viewName]) {
            try {
                await this.loadViewComponent(viewName, container, params);
            } catch (error) {
                console.error(`Failed to load view ${viewName}:`, error);
                this.showError(`Failed to load ${viewName} view`);
            }
        } else {
            // View already loaded, just refresh if needed
            if (this.views[viewName].onNavigate) {
                this.views[viewName].onNavigate(params);
            }
        }
    }

    async loadViewComponent(viewName, container, params) {
        switch (viewName) {
            case 'dictionary':
                const { Dictionary } = await import('./components/dictionary.js');
                this.views.dictionary = new Dictionary(container, dbManager);
                break;

            case 'query-builder':
                const { QueryBuilder } = await import('./components/query-builder.js');
                this.views['query-builder'] = new QueryBuilder(container, dbManager);
                break;

            case 'sql-editor':
                const { SQLEditor } = await import('./components/sql-editor.js');
                this.views['sql-editor'] = new SQLEditor(container, dbManager);
                break;

            case 'about':
                this.loadAboutPage(container);
                break;

            default:
                container.innerHTML = '<div class="error">Page not found</div>';
        }
    }

    loadAboutPage(container) {
        container.innerHTML = `
            <div class="about-content">
                <h1>KEMET Data Explorer</h1>

                <section>
                    <h2>About This Database</h2>
                    <p>
                        The KEMET Data project integrates Ancient Egyptian and Coptic linguistic data from
                        8 authoritative sources into a unified, queryable database.
                    </p>
                    <ul>
                        <li><strong>Corpus Database (741 MB):</strong> 1.5M tokens from 1,552 documents across 92 collections</li>
                        <li><strong>Lexicon Database (84 MB):</strong> 33,259 lemmas with 104,659 forms and morphological data</li>
                    </ul>
                </section>

                <section>
                    <h2>Data Sources</h2>
                    <ul>
                        <li><strong>ORAEC:</strong> Corpus texts (CC0)</li>
                        <li><strong>BBAW TLA:</strong> Egyptian lexicon (CC BY-SA 4.0)</li>
                        <li><strong>CDO:</strong> Coptic lexicon (CC BY-SA 4.0)</li>
                        <li><strong>Coptic Scriptorium:</strong> Annotated Coptic texts (CC BY 4.0)</li>
                        <li><strong>Universal Dependencies:</strong> UD-Coptic treebank (CC BY-SA 4.0)</li>
                        <li><strong>Coptic etymologies:</strong> ORAEC etymology data (CC0)</li>
                        <li><strong>ANE-NOW:</strong> Horner's NT concordance (Public Domain)</li>
                    </ul>
                </section>

                <section>
                    <h2>Features</h2>
                    <ul>
                        <li><strong>Dictionary:</strong> Search lemmas by Coptic, Egyptian, or Greek forms with autocomplete</li>
                        <li><strong>Query Builder:</strong> Parameterized queries for concordances, frequency analysis, and more</li>
                        <li><strong>SQL Editor:</strong> Full SQL access with example queries from the cookbook</li>
                        <li><strong>Export:</strong> Download results as CSV or JSON</li>
                        <li><strong>Cross-references:</strong> Direct links to TLA, ORAEC, CDO, and Scriptorium</li>
                    </ul>
                </section>

                <section>
                    <h2>Technical Details</h2>
                    <p>
                        This interface runs entirely in your browser using DuckDB-WASM. Databases are
                        downloaded once and cached in IndexedDB for instant access on future visits.
                    </p>
                    <p>
                        <strong>Browser Requirements:</strong> Chrome 90+, Firefox 88+, or Safari 14+ with
                        ES2020 modules, WebAssembly, and IndexedDB support.
                    </p>
                </section>

                <section>
                    <h2>License and Attribution</h2>
                    <p>
                        KEMET Data is released under <strong>CC BY-SA 4.0</strong>. Individual sources
                        retain their original licenses (see above).
                    </p>
                    <p>
                        <strong>Authors:</strong> Tiago Tresoldi and Marwan Kilani
                    </p>
                    <p>
                        When citing this resource, please include:<br>
                        <em>KEMET Data: Integrated Ancient Egyptian and Coptic Linguistic Database</em><br>
                        Available at: <a href="https://github.com/tresoldi/kemet-data" target="_blank">https://github.com/tresoldi/kemet-data</a>
                    </p>
                </section>

                <section>
                    <h2>External Resources</h2>
                    <ul>
                        <li><a href="https://thesaurus-linguae-aegyptiae.de/" target="_blank">Thesaurus Linguae Aegyptiae (TLA)</a></li>
                        <li><a href="https://oraec.github.io/" target="_blank">ORAEC Project</a></li>
                        <li><a href="https://coptic-dictionary.org/" target="_blank">Coptic Dictionary Online (CDO)</a></li>
                        <li><a href="https://copticscriptorium.org/" target="_blank">Coptic Scriptorium</a></li>
                    </ul>
                </section>
            </div>
        `;

        this.views.about = { loaded: true };
    }

    showError(message) {
        const appContainer = document.getElementById('app');
        const errorDiv = document.createElement('div');
        errorDiv.className = 'error-message';
        errorDiv.textContent = message;

        // Insert at top of app container
        if (appContainer && appContainer.firstChild) {
            appContainer.insertBefore(errorDiv, appContainer.firstChild);
        }

        // Auto-remove after 10 seconds
        setTimeout(() => {
            errorDiv.remove();
        }, 10000);
    }
}

// Initialize app when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        const app = new App();
        app.initialize();
    });
} else {
    const app = new App();
    app.initialize();
}

export default App;

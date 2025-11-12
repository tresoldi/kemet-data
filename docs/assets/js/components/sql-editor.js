/**
 * SQL Editor Component
 * Advanced SQL interface with cookbook examples
 */

import { getAllQueries, getQueriesByCategory, getCategories, getCategoryName } from '../utils/cookbook.js';
import { ResultsTable } from './results-table.js';

export class SQLEditor {
    constructor(container, dbManager) {
        this.container = container;
        this.dbManager = dbManager;
        this.resultsTable = null;
        this.currentSQL = '';

        this.render();
    }

    render() {
        const categories = getCategories();

        this.container.innerHTML = `
            <div class="sql-editor-content">
                <h2>SQL Editor</h2>
                <p class="text-muted">
                    Write custom SQL queries or select from the cookbook examples.
                    Both corpus and lexicon databases are available.
                </p>

                <!-- Cookbook -->
                <div class="cookbook-section">
                    <h3>Query Cookbook</h3>
                    <p class="text-muted">Choose an example query to get started</p>

                    <div class="cookbook-categories">
                        ${categories.map(cat => this.renderCategory(cat)).join('')}
                    </div>
                </div>

                <!-- SQL Editor Area -->
                <div class="editor-section">
                    <div class="editor-toolbar">
                        <div class="toolbar-left">
                            <button id="execute-sql-btn" class="primary">Execute (Ctrl+Enter)</button>
                            <button id="clear-editor-btn" class="secondary">Clear</button>
                            <button id="format-sql-btn" class="secondary">Format SQL</button>
                        </div>
                        <div class="toolbar-right">
                            <span class="text-muted" id="query-info"></span>
                        </div>
                    </div>

                    <div class="editor-area">
                        <textarea
                            id="sql-textarea"
                            placeholder="-- Enter your SQL query here
-- Example:
-- SELECT * FROM corpus.documents LIMIT 10;
-- SELECT * FROM lexicon.lemmas WHERE language = 'cop' LIMIT 10;
"
                            spellcheck="false"
                        ></textarea>
                    </div>
                </div>

                <!-- Schema Reference -->
                <details class="schema-reference">
                    <summary><strong>Database Schema Quick Reference</strong></summary>
                    <div class="schema-content">
                        <h4>Corpus Database (corpus.*)</h4>
                        <ul>
                            <li><code>corpus.collections</code> - Collections (92 collections)</li>
                            <li><code>corpus.documents</code> - Documents (1,552 documents)</li>
                            <li><code>corpus.segments</code> - Text segments (252,826 segments)</li>
                            <li><code>corpus.tokens</code> - Tokens (1,558,260 tokens)</li>
                        </ul>

                        <h4>Lexicon Database (lexicon.*)</h4>
                        <ul>
                            <li><code>lexicon.lemmas</code> - Lemmas (33,259 lemmas)</li>
                            <li><code>lexicon.forms</code> - Morphological forms (104,659 forms)</li>
                            <li><code>lexicon.lemma_attestations</code> - Attestation statistics</li>
                            <li><code>lexicon.etymology_relations</code> - Etymology data (1,750 relations)</li>
                            <li><code>lexicon.cdo_mappings</code> - CDO cross-references (9,493 mappings)</li>
                        </ul>

                        <p class="text-muted">
                            See <a href="https://github.com/tresoldi/kemet-data/blob/main/DATABASE.md" target="_blank">DATABASE.md</a> for complete schema.
                        </p>
                    </div>
                </details>

                <!-- Results -->
                <div id="sql-results" class="query-results">
                    <!-- Results will be displayed here -->
                </div>
            </div>
        `;

        this.attachEventListeners();
    }

    renderCategory(category) {
        const queries = getQueriesByCategory(category);

        return `
            <div class="cookbook-category">
                <h4>${getCategoryName(category)}</h4>
                <div class="cookbook-queries">
                    ${queries.map(query => `
                        <button
                            class="cookbook-query-btn"
                            data-query-id="${query.id}"
                            title="${query.description}"
                        >
                            ${query.name}
                        </button>
                    `).join('')}
                </div>
            </div>
        `;
    }

    attachEventListeners() {
        // Cookbook query buttons
        const queryButtons = this.container.querySelectorAll('.cookbook-query-btn');
        queryButtons.forEach(btn => {
            btn.addEventListener('click', () => {
                const queryId = btn.dataset.queryId;
                this.loadCookbookQuery(queryId);
            });
        });

        // SQL textarea
        const textarea = document.getElementById('sql-textarea');
        if (textarea) {
            // Ctrl+Enter to execute
            textarea.addEventListener('keydown', (e) => {
                if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                    e.preventDefault();
                    this.executeSQL();
                }
            });

            // Update query info on input
            textarea.addEventListener('input', () => {
                this.updateQueryInfo();
            });
        }

        // Execute button
        const executeBtn = document.getElementById('execute-sql-btn');
        if (executeBtn) {
            executeBtn.addEventListener('click', () => {
                this.executeSQL();
            });
        }

        // Clear button
        const clearBtn = document.getElementById('clear-editor-btn');
        if (clearBtn) {
            clearBtn.addEventListener('click', () => {
                this.clearEditor();
            });
        }

        // Format SQL button
        const formatBtn = document.getElementById('format-sql-btn');
        if (formatBtn) {
            formatBtn.addEventListener('click', () => {
                this.formatSQL();
            });
        }
    }

    loadCookbookQuery(queryId) {
        const query = getAllQueries().find(q => q.id === queryId);
        if (!query) return;

        const textarea = document.getElementById('sql-textarea');
        if (textarea) {
            // Add comment header
            const header = `-- ${query.name}\n-- ${query.description}\n\n`;
            textarea.value = header + query.sql;
            this.currentSQL = textarea.value;
            this.updateQueryInfo();

            // Scroll to editor
            textarea.scrollIntoView({ behavior: 'smooth', block: 'center' });
            textarea.focus();
        }
    }

    async executeSQL() {
        const textarea = document.getElementById('sql-textarea');
        const executeBtn = document.getElementById('execute-sql-btn');
        const resultsDiv = document.getElementById('sql-results');

        if (!textarea) return;

        const sql = textarea.value.trim();
        if (!sql) {
            alert('Please enter a SQL query');
            return;
        }

        try {
            // Disable button
            if (executeBtn) {
                executeBtn.disabled = true;
                executeBtn.textContent = 'Executing...';
            }

            // Clear previous results
            resultsDiv.innerHTML = '<div class="text-muted">Executing query...</div>';

            const startTime = performance.now();

            // Execute query
            const results = await this.dbManager.query(sql);

            const endTime = performance.now();
            const executionTime = ((endTime - startTime) / 1000).toFixed(3);

            // Display results
            if (!this.resultsTable) {
                this.resultsTable = new ResultsTable(resultsDiv);
            }

            this.resultsTable.render(results, {
                showExport: true,
                maxHeight: '600px',
                emptyMessage: 'Query executed successfully but returned no results'
            });

            // Show execution time
            const queryInfo = document.getElementById('query-info');
            if (queryInfo) {
                queryInfo.innerHTML = `
                    <span class="success-text">
                        ✓ Query executed in ${executionTime}s
                        (${results.length} ${results.length === 1 ? 'row' : 'rows'})
                    </span>
                `;
            }

        } catch (error) {
            console.error('SQL execution error:', error);

            resultsDiv.innerHTML = `
                <div class="error-message">
                    <strong>SQL Error:</strong><br>
                    ${this.escapeHtml(error.message)}
                </div>
            `;

            const queryInfo = document.getElementById('query-info');
            if (queryInfo) {
                queryInfo.innerHTML = '<span class="error-text">✗ Query failed</span>';
            }

        } finally {
            // Re-enable button
            if (executeBtn) {
                executeBtn.disabled = false;
                executeBtn.textContent = 'Execute (Ctrl+Enter)';
            }
        }
    }

    clearEditor() {
        const textarea = document.getElementById('sql-textarea');
        const resultsDiv = document.getElementById('sql-results');
        const queryInfo = document.getElementById('query-info');

        if (textarea) {
            textarea.value = '';
            this.currentSQL = '';
        }

        if (resultsDiv) {
            resultsDiv.innerHTML = '';
        }

        if (queryInfo) {
            queryInfo.textContent = '';
        }

        if (this.resultsTable) {
            this.resultsTable.clear();
        }
    }

    formatSQL() {
        const textarea = document.getElementById('sql-textarea');
        if (!textarea) return;

        let sql = textarea.value;

        // Basic SQL formatting
        // This is a simple formatter; a real implementation would use a proper SQL parser

        // Preserve comments
        const lines = sql.split('\n');
        const formatted = [];

        let inComment = false;
        let indent = 0;

        for (let line of lines) {
            const trimmed = line.trim();

            // Skip empty lines
            if (!trimmed) {
                formatted.push('');
                continue;
            }

            // Preserve comments
            if (trimmed.startsWith('--')) {
                formatted.push(trimmed);
                continue;
            }

            // Simple indentation logic
            if (/^(SELECT|FROM|WHERE|GROUP BY|ORDER BY|HAVING|LIMIT)/i.test(trimmed)) {
                formatted.push('  '.repeat(Math.max(0, indent)) + trimmed.toUpperCase());
            } else if (/^(JOIN|LEFT JOIN|RIGHT JOIN|INNER JOIN)/i.test(trimmed)) {
                formatted.push('  '.repeat(indent) + trimmed.toUpperCase());
            } else {
                formatted.push('  '.repeat(indent + 1) + trimmed);
            }
        }

        textarea.value = formatted.join('\n');
        this.updateQueryInfo();
    }

    updateQueryInfo() {
        const textarea = document.getElementById('sql-textarea');
        const queryInfo = document.getElementById('query-info');

        if (!textarea || !queryInfo) return;

        const sql = textarea.value.trim();
        const lines = sql.split('\n').length;
        const chars = sql.length;

        if (sql) {
            queryInfo.textContent = `${lines} ${lines === 1 ? 'line' : 'lines'}, ${chars} ${chars === 1 ? 'char' : 'chars'}`;
        } else {
            queryInfo.textContent = '';
        }
    }

    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    onNavigate(params) {
        // Handle navigation with query ID parameter
        if (params && params[0]) {
            this.loadCookbookQuery(params[0]);
        }
    }
}

export default SQLEditor;

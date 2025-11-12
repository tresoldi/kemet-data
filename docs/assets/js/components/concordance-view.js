/**
 * Concordance View Component
 * Displays KWIC (Key Word In Context) format with expandable rows
 */

import { exportToCSV, exportToJSON } from '../utils/export.js';
import config from '../config.js';

// Helper function to escape SQL strings
function escapeSQL(str) {
    if (!str) return '';
    return String(str).replace(/'/g, "''");
}

export class ConcordanceView {
    constructor(container, dbManager) {
        this.container = container;
        this.dbManager = dbManager;
        this.currentResults = [];
        this.expandedRows = new Set();
    }

    async loadConcordance(lemmaId, options = {}) {
        const {
            limit = config.ui.maxResultsDefault,
            collection = null,
            dialect = null,
            dateFrom = null,
            dateTo = null
        } = options;

        try {
            const results = await this.fetchConcordance(lemmaId, limit, {
                collection,
                dialect,
                dateFrom,
                dateTo
            });

            this.currentResults = results;
            this.render(results, lemmaId);
        } catch (error) {
            console.error('Error loading concordance:', error);
            this.showError(`Failed to load concordance: ${error.message}`);
        }
    }

    async fetchConcordance(lemmaId, limit, filters) {
        // Split into multiple queries to avoid DuckDB-WASM "_setThrew" errors
        // Note: The table is called token_instances, not tokens

        // Step 1: Fetch token instances
        let tokensSql = `
            SELECT
                token_id,
                segment_id,
                document_id,
                form,
                lemma_id,
                lang,
                "order" as position_in_segment
            FROM corpus.token_instances
            WHERE lemma_id = '${escapeSQL(lemmaId)}'
            LIMIT ${limit}
        `;

        const tokens = await this.dbManager.query(tokensSql);
        if (tokens.length === 0) {
            return [];
        }

        // Step 2: Fetch segment and document info
        const segmentIds = tokens.map(t => `'${escapeSQL(t.segment_id)}'`).join(',');
        let contextSql = `
            SELECT
                s.segment_id,
                s.text as segment_text,
                s.sequence,
                d.document_id,
                d.title as document_title,
                d.dialect,
                d.collection_id
            FROM corpus.segments s
            JOIN corpus.documents d ON s.document_id = d.document_id
            WHERE s.segment_id IN (${segmentIds})
        `;

        // Apply filters
        if (filters.dialect) {
            contextSql += ` AND d.dialect = '${escapeSQL(filters.dialect)}'`;
        }

        const contexts = await this.dbManager.query(contextSql);

        // Step 3: Fetch collection names
        const collectionIds = [...new Set(contexts.map(c => c.collection_id).filter(Boolean))];
        let collections = {};
        if (collectionIds.length > 0) {
            const collectionIdList = collectionIds.map(id => `'${escapeSQL(id)}'`).join(',');
            let collectionSql = `
                SELECT collection_id, name
                FROM corpus.collections
                WHERE collection_id IN (${collectionIdList})
            `;

            // Apply collection filter
            if (filters.collection) {
                collectionSql += ` AND collection_id = '${escapeSQL(filters.collection)}'`;
            }

            const collectionResults = await this.dbManager.query(collectionSql);
            collections = Object.fromEntries(collectionResults.map(c => [c.collection_id, c.name]));
        }

        // Merge results
        return tokens.map(token => {
            const context = contexts.find(c => c.segment_id === token.segment_id);
            if (!context) return null;

            // Skip if collection filter doesn't match
            if (filters.collection && context.collection_id !== filters.collection) {
                return null;
            }

            return {
                token_id: token.token_id,
                segment_id: token.segment_id,
                form: token.form,
                morphology: token.morphology,
                position_in_segment: token.position_in_segment,
                segment_text: context.segment_text || '',
                sequence: context.sequence || 0,
                document_id: context.document_id || '',
                document_title: context.document_title || '',
                dialect: context.dialect || '',
                collection_id: context.collection_id || '',
                collection_name: collections[context.collection_id] || ''
            };
        }).filter(Boolean); // Remove nulls from filtered results
    }

    async fetchFullSegment(segmentId) {
        const sql = `
            SELECT
                s.segment_id,
                s.text,
                s.sequence,
                d.title as document_title,
                d.document_id
            FROM corpus.segments s
            JOIN corpus.documents d ON s.document_id = d.document_id
            WHERE s.segment_id = '${escapeSQL(segmentId)}'
        `;

        const results = await this.dbManager.query(sql);
        return results.length > 0 ? results[0] : null;
    }

    render(results, lemmaId) {
        if (!results || results.length === 0) {
            this.container.innerHTML = `
                <div class="concordance-container">
                    <h3>Concordance</h3>
                    <p class="text-muted">No results found for this lemma.</p>
                </div>
            `;
            return;
        }

        this.container.innerHTML = `
            <div class="concordance-container">
                <div class="results-controls">
                    <div class="results-info">
                        <strong>${results.length}</strong> occurrences shown
                    </div>
                    <div class="export-buttons">
                        <button id="export-csv-btn" class="secondary">Export CSV</button>
                        <button id="export-json-btn" class="secondary">Export JSON</button>
                    </div>
                </div>

                <table class="kwic-table">
                    <thead>
                        <tr>
                            <th class="kwic-left">Left Context</th>
                            <th class="kwic-keyword">Keyword</th>
                            <th class="kwic-right">Right Context</th>
                            <th>Source</th>
                        </tr>
                    </thead>
                    <tbody id="concordance-tbody">
                        ${this.renderRows(results)}
                    </tbody>
                </table>
            </div>
        `;

        this.attachEventListeners();
    }

    renderRows(results) {
        return results.map((ctx, index) => {
            const kwic = this.buildKWIC(ctx);
            const isExpanded = this.expandedRows.has(ctx.token_id);

            return `
                <tr class="kwic-expandable ${isExpanded ? 'kwic-expanded' : ''}" data-token-id="${ctx.token_id}" data-segment-id="${ctx.segment_id}">
                    <td class="kwic-left">${this.escapeHtml(kwic.left)}</td>
                    <td class="kwic-keyword">${this.escapeHtml(kwic.keyword)}</td>
                    <td class="kwic-right">${this.escapeHtml(kwic.right)}</td>
                    <td class="kwic-source">
                        <span class="text-muted" title="${ctx.document_title}">${this.truncate(ctx.document_title, 30)}</span>
                        ${ctx.morphology ? `<br><span class="badge-small">${ctx.morphology}</span>` : ''}
                    </td>
                </tr>
                ${isExpanded ? this.renderExpandedRow(ctx) : ''}
            `;
        }).join('');
    }

    renderExpandedRow(ctx) {
        return `
            <tr class="kwic-expanded-content" data-token-id="${ctx.token_id}">
                <td colspan="4">
                    <div class="expanded-context">
                        <strong>Full Segment:</strong>
                        <p>${this.escapeHtml(ctx.segment_text)}</p>
                        <div class="expanded-meta">
                            <span>Document: ${ctx.document_title}</span>
                            ${ctx.dialect ? `<span>| Dialect: ${ctx.dialect}</span>` : ''}
                            ${ctx.collection_name ? `<span>| Collection: ${ctx.collection_name}</span>` : ''}
                        </div>
                    </div>
                </td>
            </tr>
        `;
    }

    attachEventListeners() {
        // Export buttons
        const csvBtn = document.getElementById('export-csv-btn');
        if (csvBtn) {
            csvBtn.addEventListener('click', () => this.exportResults('csv'));
        }

        const jsonBtn = document.getElementById('export-json-btn');
        if (jsonBtn) {
            jsonBtn.addEventListener('click', () => this.exportResults('json'));
        }

        // Row expansion
        const tbody = document.getElementById('concordance-tbody');
        if (tbody) {
            tbody.addEventListener('click', async (e) => {
                const row = e.target.closest('.kwic-expandable');
                if (!row) return;

                const tokenId = row.dataset.tokenId;
                const segmentId = row.dataset.segmentId;

                if (this.expandedRows.has(tokenId)) {
                    // Collapse
                    this.expandedRows.delete(tokenId);
                } else {
                    // Expand - fetch full segment if needed
                    this.expandedRows.add(tokenId);
                }

                // Re-render rows
                tbody.innerHTML = this.renderRows(this.currentResults);
            });
        }
    }

    buildKWIC(context) {
        const text = context.segment_text || '';
        const form = context.form;
        const position = context.position_in_segment || 0;

        // Find the keyword in the text
        const index = text.indexOf(form);

        if (index >= 0) {
            const left = text.substring(0, index);
            const keyword = form;
            const right = text.substring(index + form.length);

            // Extract context windows
            const leftContext = left.length > config.ui.concordanceContextWidth
                ? '...' + left.substring(left.length - config.ui.concordanceContextWidth)
                : left;

            const rightContext = right.length > config.ui.concordanceContextWidth
                ? right.substring(0, config.ui.concordanceContextWidth) + '...'
                : right;

            return {
                left: leftContext,
                keyword,
                right: rightContext
            };
        }

        // Fallback if form not found in text
        return {
            left: '',
            keyword: form,
            right: text.substring(0, config.ui.concordanceContextWidth * 2)
        };
    }

    exportResults(format) {
        if (!this.currentResults || this.currentResults.length === 0) {
            alert('No results to export');
            return;
        }

        // Prepare export data
        const exportData = this.currentResults.map(ctx => {
            const kwic = this.buildKWIC(ctx);
            return {
                left_context: kwic.left,
                keyword: kwic.keyword,
                right_context: kwic.right,
                full_segment: ctx.segment_text,
                document: ctx.document_title,
                collection: ctx.collection_name,
                dialect: ctx.dialect,
                morphology: ctx.morphology,
                token_id: ctx.token_id,
                segment_id: ctx.segment_id
            };
        });

        if (format === 'csv') {
            exportToCSV(exportData, 'concordance.csv');
        } else if (format === 'json') {
            exportToJSON(exportData, 'concordance.json');
        }
    }

    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    truncate(text, maxLength) {
        if (!text || text.length <= maxLength) return text;
        return text.substring(0, maxLength) + '...';
    }

    showError(message) {
        this.container.innerHTML = `
            <div class="error-message">
                ${message}
            </div>
        `;
    }
}

export default ConcordanceView;

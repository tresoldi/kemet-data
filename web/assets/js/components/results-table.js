/**
 * Results Table Component
 * Displays query results in a sortable, interactive table
 */

import { exportToCSV, exportToJSON } from '../utils/export.js';

export class ResultsTable {
    constructor(container) {
        this.container = container;
        this.data = [];
        this.columns = [];
        this.sortColumn = null;
        this.sortDirection = 'asc';
    }

    render(data, options = {}) {
        this.data = data || [];
        const {
            showExport = true,
            maxHeight = '600px',
            emptyMessage = 'No results to display'
        } = options;

        if (!this.data || this.data.length === 0) {
            this.container.innerHTML = `
                <div class="results-empty">
                    <p class="text-muted">${emptyMessage}</p>
                </div>
            `;
            return;
        }

        // Extract columns from first row
        this.columns = Object.keys(this.data[0]);

        this.container.innerHTML = `
            <div class="results-container">
                ${showExport ? this.renderControls() : ''}
                <div class="results-table-wrapper" style="max-height: ${maxHeight}; overflow: auto;">
                    <table class="results-table">
                        <thead>
                            ${this.renderHeader()}
                        </thead>
                        <tbody>
                            ${this.renderRows()}
                        </tbody>
                    </table>
                </div>
            </div>
        `;

        this.attachEventListeners();
    }

    renderControls() {
        return `
            <div class="results-controls">
                <div class="results-info">
                    <strong>${this.data.length}</strong> ${this.data.length === 1 ? 'row' : 'rows'}
                    ${this.columns.length > 0 ? `× <strong>${this.columns.length}</strong> ${this.columns.length === 1 ? 'column' : 'columns'}` : ''}
                </div>
                <div class="export-buttons">
                    <button id="export-csv-btn" class="secondary">Export CSV</button>
                    <button id="export-json-btn" class="secondary">Export JSON</button>
                </div>
            </div>
        `;
    }

    renderHeader() {
        return `
            <tr>
                ${this.columns.map(col => {
                    const isSorted = this.sortColumn === col;
                    const arrow = isSorted ? (this.sortDirection === 'asc' ? ' ▲' : ' ▼') : '';
                    return `
                        <th class="sortable" data-column="${col}">
                            ${this.formatColumnName(col)}${arrow}
                        </th>
                    `;
                }).join('')}
            </tr>
        `;
    }

    renderRows() {
        const sortedData = this.sortColumn ? this.sortData(this.data) : this.data;

        return sortedData.map((row, index) => `
            <tr>
                ${this.columns.map(col => `
                    <td>${this.formatCellValue(row[col], col)}</td>
                `).join('')}
            </tr>
        `).join('');
    }

    sortData(data) {
        const sorted = [...data];
        const column = this.sortColumn;
        const direction = this.sortDirection;

        sorted.sort((a, b) => {
            let valA = a[column];
            let valB = b[column];

            // Handle nulls
            if (valA === null || valA === undefined) return 1;
            if (valB === null || valB === undefined) return -1;

            // Handle numbers
            if (typeof valA === 'number' && typeof valB === 'number') {
                return direction === 'asc' ? valA - valB : valB - valA;
            }

            // Handle strings
            const strA = String(valA).toLowerCase();
            const strB = String(valB).toLowerCase();

            if (strA < strB) return direction === 'asc' ? -1 : 1;
            if (strA > strB) return direction === 'asc' ? 1 : -1;
            return 0;
        });

        return sorted;
    }

    attachEventListeners() {
        // Export buttons
        const csvBtn = this.container.querySelector('#export-csv-btn');
        if (csvBtn) {
            csvBtn.addEventListener('click', () => this.exportData('csv'));
        }

        const jsonBtn = this.container.querySelector('#export-json-btn');
        if (jsonBtn) {
            jsonBtn.addEventListener('click', () => this.exportData('json'));
        }

        // Column sorting
        const headers = this.container.querySelectorAll('th.sortable');
        headers.forEach(header => {
            header.addEventListener('click', () => {
                const column = header.dataset.column;
                this.handleSort(column);
            });
        });
    }

    handleSort(column) {
        if (this.sortColumn === column) {
            // Toggle direction
            this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            // New column
            this.sortColumn = column;
            this.sortDirection = 'asc';
        }

        // Re-render table body and header
        const tbody = this.container.querySelector('tbody');
        const thead = this.container.querySelector('thead');

        if (tbody) {
            tbody.innerHTML = this.renderRows();
        }

        if (thead) {
            thead.innerHTML = this.renderHeader();
        }

        // Re-attach sort listeners
        const headers = this.container.querySelectorAll('th.sortable');
        headers.forEach(header => {
            header.addEventListener('click', () => {
                const col = header.dataset.column;
                this.handleSort(col);
            });
        });
    }

    formatColumnName(column) {
        // Convert snake_case to Title Case
        return column
            .split('_')
            .map(word => word.charAt(0).toUpperCase() + word.slice(1))
            .join(' ');
    }

    formatCellValue(value, columnName) {
        // Handle null/undefined
        if (value === null || value === undefined) {
            return '<span class="text-muted">—</span>';
        }

        // Handle arrays
        if (Array.isArray(value)) {
            return value.join(', ');
        }

        // Handle objects/JSON
        if (typeof value === 'object') {
            return `<code>${JSON.stringify(value, null, 2)}</code>`;
        }

        // Handle numbers with formatting
        if (typeof value === 'number') {
            // Format large numbers with commas
            if (Number.isInteger(value) && Math.abs(value) >= 1000) {
                return value.toLocaleString();
            }
            // Format decimals
            if (!Number.isInteger(value)) {
                return value.toFixed(4);
            }
            return value.toString();
        }

        // Handle booleans
        if (typeof value === 'boolean') {
            return value ? '✓' : '✗';
        }

        // Handle strings
        const strValue = String(value);

        // Detect and style special content types
        if (this.isLemmaId(strValue)) {
            return `<span class="lemma-id">${this.escapeHtml(strValue)}</span>`;
        }

        if (this.isCoptic(strValue)) {
            return `<span class="coptic-text">${this.escapeHtml(strValue)}</span>`;
        }

        if (this.isTransliteration(strValue)) {
            return `<span class="transliteration-text">${this.escapeHtml(strValue)}</span>`;
        }

        // Truncate very long strings
        if (strValue.length > 200) {
            return `<span title="${this.escapeHtml(strValue)}">${this.escapeHtml(strValue.substring(0, 200))}...</span>`;
        }

        return this.escapeHtml(strValue);
    }

    isLemmaId(str) {
        return /^(cop|egy|grc|he):lemma:/.test(str);
    }

    isCoptic(str) {
        return /[\u2C80-\u2CFF]/.test(str);
    }

    isTransliteration(str) {
        return /[ꜣꜥḥḫẖḏḍṯṭśšḳḵ]/.test(str);
    }

    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    exportData(format) {
        if (!this.data || this.data.length === 0) {
            alert('No data to export');
            return;
        }

        if (format === 'csv') {
            exportToCSV(this.data, 'query-results.csv');
        } else if (format === 'json') {
            exportToJSON(this.data, 'query-results.json');
        }
    }

    clear() {
        this.container.innerHTML = '';
        this.data = [];
        this.columns = [];
        this.sortColumn = null;
        this.sortDirection = 'asc';
    }
}

export default ResultsTable;

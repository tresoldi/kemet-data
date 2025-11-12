/**
 * Query Builder Component
 * Parameterized query interface for intermediate users
 */

import { getAllTemplates, getTemplate } from '../utils/sql-templates.js';
import { ResultsTable } from './results-table.js';

export class QueryBuilder {
    constructor(container, dbManager) {
        this.container = container;
        this.dbManager = dbManager;
        this.currentTemplate = null;
        this.resultsTable = null;

        this.render();
    }

    render() {
        const templates = getAllTemplates();

        this.container.innerHTML = `
            <div class="query-builder-content">
                <h2>Query Builder</h2>
                <p class="text-muted">
                    Choose a query template and fill in the parameters. The SQL query will be generated automatically.
                </p>

                <!-- Template Selection -->
                <div class="form-group">
                    <label for="template-select">Query Template</label>
                    <select id="template-select">
                        <option value="">-- Select a query type --</option>
                        ${this.renderTemplateOptions(templates)}
                    </select>
                </div>

                <!-- Template Description -->
                <div id="template-description" class="hidden">
                    <!-- Will be populated when template selected -->
                </div>

                <!-- Parameter Form -->
                <div id="parameter-form" class="query-form hidden">
                    <!-- Will be populated dynamically -->
                </div>

                <!-- SQL Preview -->
                <div id="sql-preview-container" class="hidden">
                    <h3>Generated SQL</h3>
                    <div class="query-preview" id="sql-preview">
                        <!-- SQL will be shown here -->
                    </div>
                    <div class="form-group">
                        <button id="execute-btn" class="primary">Execute Query</button>
                        <button id="clear-btn" class="secondary">Clear</button>
                    </div>
                </div>

                <!-- Results -->
                <div id="query-results" class="query-results">
                    <!-- Results will be displayed here -->
                </div>
            </div>
        `;

        this.attachEventListeners();
    }

    renderTemplateOptions(templates) {
        // Group by category
        const grouped = {};
        templates.forEach(t => {
            if (!grouped[t.category]) {
                grouped[t.category] = [];
            }
            grouped[t.category].push(t);
        });

        return Object.entries(grouped).map(([category, templates]) => `
            <optgroup label="${this.formatCategory(category)}">
                ${templates.map(t => `
                    <option value="${t.id}">${t.name}</option>
                `).join('')}
            </optgroup>
        `).join('');
    }

    formatCategory(category) {
        return category.charAt(0).toUpperCase() + category.slice(1);
    }

    attachEventListeners() {
        const templateSelect = document.getElementById('template-select');
        if (templateSelect) {
            templateSelect.addEventListener('change', (e) => {
                this.handleTemplateChange(e.target.value);
            });
        }

        const clearBtn = document.getElementById('clear-btn');
        if (clearBtn) {
            clearBtn.addEventListener('click', () => {
                this.clearResults();
            });
        }
    }

    handleTemplateChange(templateId) {
        // Clear previous state
        this.clearResults();
        document.getElementById('sql-preview-container').classList.add('hidden');

        if (!templateId) {
            document.getElementById('template-description').classList.add('hidden');
            document.getElementById('parameter-form').classList.add('hidden');
            return;
        }

        const template = getTemplate(templateId);
        if (!template) return;

        this.currentTemplate = template;

        // Show description
        const descDiv = document.getElementById('template-description');
        descDiv.innerHTML = `
            <div class="template-description">
                <h3>${template.name}</h3>
                <p>${template.description}</p>
            </div>
        `;
        descDiv.classList.remove('hidden');

        // Render parameter form
        this.renderParameterForm(template);

        // Show parameter form
        document.getElementById('parameter-form').classList.remove('hidden');
    }

    renderParameterForm(template) {
        const formDiv = document.getElementById('parameter-form');

        formDiv.innerHTML = `
            <h3>Parameters</h3>
            ${template.parameters.map(param => this.renderParameterField(param)).join('')}
            <div class="form-group">
                <button id="generate-sql-btn" class="primary">Generate SQL</button>
            </div>
        `;

        // Attach listeners
        const generateBtn = document.getElementById('generate-sql-btn');
        if (generateBtn) {
            generateBtn.addEventListener('click', () => {
                this.handleGenerateSQL();
            });
        }

        // Attach listeners for parameter changes
        template.parameters.forEach(param => {
            const input = document.getElementById(`param-${param.name}`);
            if (input) {
                input.addEventListener('input', () => {
                    this.validateForm();
                });
                input.addEventListener('change', () => {
                    this.validateForm();
                });
            }
        });
    }

    renderParameterField(param) {
        const isRequired = param.required ? 'required' : '';
        const helpText = param.helpText ? `<small class="text-muted">${param.helpText}</small>` : '';

        switch (param.type) {
            case 'text':
            case 'lemma_search':
                return `
                    <div class="form-group">
                        <label for="param-${param.name}">
                            ${param.label}
                            ${param.required ? '<span class="required">*</span>' : ''}
                        </label>
                        <input
                            type="text"
                            id="param-${param.name}"
                            name="${param.name}"
                            placeholder="${param.placeholder || ''}"
                            ${isRequired}
                            value="${param.default || ''}"
                        />
                        ${helpText}
                    </div>
                `;

            case 'number':
                return `
                    <div class="form-group">
                        <label for="param-${param.name}">
                            ${param.label}
                            ${param.required ? '<span class="required">*</span>' : ''}
                        </label>
                        <input
                            type="number"
                            id="param-${param.name}"
                            name="${param.name}"
                            min="${param.min || 0}"
                            max="${param.max || ''}"
                            value="${param.default || ''}"
                            ${isRequired}
                        />
                        ${helpText}
                    </div>
                `;

            case 'select':
                return `
                    <div class="form-group">
                        <label for="param-${param.name}">
                            ${param.label}
                            ${param.required ? '<span class="required">*</span>' : ''}
                        </label>
                        <select id="param-${param.name}" name="${param.name}" ${isRequired}>
                            ${param.allowNull ? '<option value="">-- None --</option>' : ''}
                            ${this.renderSelectOptions(param)}
                        </select>
                        ${helpText}
                    </div>
                `;

            case 'checkbox':
                return `
                    <div class="form-group">
                        <label>
                            <input
                                type="checkbox"
                                id="param-${param.name}"
                                name="${param.name}"
                                ${param.default ? 'checked' : ''}
                            />
                            ${param.label}
                        </label>
                        ${helpText}
                    </div>
                `;

            default:
                return '';
        }
    }

    renderSelectOptions(param) {
        if (Array.isArray(param.options)) {
            return param.options.map(opt => {
                const value = opt;
                const label = param.labels && param.labels[opt] ? param.labels[opt] : opt;
                const selected = param.default === opt ? 'selected' : '';
                return `<option value="${value}" ${selected}>${label}</option>`;
            }).join('');
        }

        // TODO: For dynamic options like 'collections', fetch from database
        return '<option value="">Loading...</option>';
    }

    validateForm() {
        if (!this.currentTemplate) return false;

        let isValid = true;
        const params = {};

        this.currentTemplate.parameters.forEach(param => {
            const input = document.getElementById(`param-${param.name}`);
            if (!input) return;

            let value;
            if (param.type === 'checkbox') {
                value = input.checked;
            } else {
                value = input.value.trim();
            }

            // Check required fields
            if (param.required && !value && value !== false) {
                isValid = false;
            }

            params[param.name] = value || param.default || (param.type === 'checkbox' ? false : null);
        });

        return { isValid, params };
    }

    handleGenerateSQL() {
        const validation = this.validateForm();
        if (!validation.isValid) {
            alert('Please fill in all required fields');
            return;
        }

        try {
            const sql = this.currentTemplate.buildSQL(validation.params);

            // Show SQL preview
            const previewDiv = document.getElementById('sql-preview');
            previewDiv.textContent = sql;

            const previewContainer = document.getElementById('sql-preview-container');
            previewContainer.classList.remove('hidden');

            // Attach execute button listener
            const executeBtn = document.getElementById('execute-btn');
            if (executeBtn) {
                executeBtn.onclick = () => this.executeQuery(sql);
            }
        } catch (error) {
            console.error('Error generating SQL:', error);
            alert(`Error generating SQL: ${error.message}`);
        }
    }

    async executeQuery(sql) {
        const executeBtn = document.getElementById('execute-btn');
        const resultsDiv = document.getElementById('query-results');

        try {
            // Disable button
            if (executeBtn) {
                executeBtn.disabled = true;
                executeBtn.textContent = 'Executing...';
            }

            // Execute query
            const results = await this.dbManager.query(sql);

            // Display results
            if (!this.resultsTable) {
                this.resultsTable = new ResultsTable(resultsDiv);
            }

            this.resultsTable.render(results, {
                showExport: true,
                maxHeight: '500px',
                emptyMessage: 'Query executed successfully but returned no results'
            });

        } catch (error) {
            console.error('Query execution error:', error);
            resultsDiv.innerHTML = `
                <div class="error-message">
                    <strong>Query Error:</strong> ${error.message}
                </div>
            `;
        } finally {
            // Re-enable button
            if (executeBtn) {
                executeBtn.disabled = false;
                executeBtn.textContent = 'Execute Query';
            }
        }
    }

    clearResults() {
        const resultsDiv = document.getElementById('query-results');
        if (resultsDiv) {
            resultsDiv.innerHTML = '';
        }

        if (this.resultsTable) {
            this.resultsTable.clear();
        }

        // Clear SQL preview
        const sqlPreview = document.getElementById('sql-preview');
        if (sqlPreview) {
            sqlPreview.textContent = '';
        }
    }

    onNavigate(params) {
        // Handle navigation with template parameter
        if (params && params[0]) {
            const templateSelect = document.getElementById('template-select');
            if (templateSelect) {
                templateSelect.value = params[0];
                this.handleTemplateChange(params[0]);
            }
        }
    }
}

export default QueryBuilder;

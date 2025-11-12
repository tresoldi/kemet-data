/**
 * Dictionary Component
 * Search interface for lemmas with separate inputs for Coptic, Egyptian, and Greek
 */

import { detectScript, normalizeForSearch } from '../utils/unicode-utils.js';
import config from '../config.js';

export class Dictionary {
    constructor(container, dbManager) {
        this.container = container;
        this.dbManager = dbManager;
        this.currentResults = [];
        this.selectedIndex = -1;

        this.render();
        this.attachEventListeners();
    }

    render() {
        this.container.innerHTML = `
            <div class="dictionary-content">
                <h2>Dictionary Search</h2>
                <p class="text-muted">Search for lemmas by Coptic, Egyptian (transliteration), or Greek forms.</p>

                <div class="search-container">
                    <!-- Coptic Search -->
                    <div class="form-group">
                        <label for="search-coptic">Coptic (ϣⲁϫⲉ)</label>
                        <div class="search-box">
                            <input
                                type="search"
                                id="search-coptic"
                                class="coptic-text"
                                placeholder="Type Coptic text..."
                                autocomplete="off"
                            />
                            <div id="autocomplete-coptic" class="autocomplete-results hidden"></div>
                        </div>
                    </div>

                    <!-- Egyptian Search -->
                    <div class="form-group">
                        <label for="search-egyptian">Egyptian Transliteration (jnk, ꜥnḫ)</label>
                        <div class="search-box">
                            <input
                                type="search"
                                id="search-egyptian"
                                class="transliteration-text"
                                placeholder="Type transliteration..."
                                autocomplete="off"
                            />
                            <div id="autocomplete-egyptian" class="autocomplete-results hidden"></div>
                        </div>
                    </div>

                    <!-- Greek Search -->
                    <div class="form-group">
                        <label for="search-greek">Greek (λόγος)</label>
                        <div class="search-box">
                            <input
                                type="search"
                                id="search-greek"
                                placeholder="Type Greek text..."
                                autocomplete="off"
                            />
                            <div id="autocomplete-greek" class="autocomplete-results hidden"></div>
                        </div>
                    </div>
                </div>

                <div id="search-results" class="search-results">
                    <!-- Results will be populated here -->
                </div>

                <div id="lemma-detail" class="lemma-detail-container hidden">
                    <!-- Lemma detail will be populated here -->
                </div>
            </div>
        `;
    }

    // Map language codes to element ID suffixes
    getLanguageSuffix(langCode) {
        const map = {
            'cop': 'coptic',
            'egy': 'egyptian',
            'grc': 'greek'
        };
        return map[langCode] || langCode;
    }

    attachEventListeners() {
        // Coptic search
        const copticInput = document.getElementById('search-coptic');
        copticInput.addEventListener('input', (e) => this.handleSearch(e.target.value, 'cop'));
        copticInput.addEventListener('keydown', (e) => this.handleKeyNavigation(e, 'cop'));

        // Egyptian search
        const egyptianInput = document.getElementById('search-egyptian');
        egyptianInput.addEventListener('input', (e) => this.handleSearch(e.target.value, 'egy'));
        egyptianInput.addEventListener('keydown', (e) => this.handleKeyNavigation(e, 'egy'));

        // Greek search
        const greekInput = document.getElementById('search-greek');
        greekInput.addEventListener('input', (e) => this.handleSearch(e.target.value, 'grc'));
        greekInput.addEventListener('keydown', (e) => this.handleKeyNavigation(e, 'grc'));
    }

    async handleSearch(query, language) {
        if (!query || query.length < 2) {
            this.hideAutocomplete(language);
            return;
        }

        const normalized = normalizeForSearch(query);

        try {
            // Search lemmas by language
            // Note: Using direct string interpolation as DuckDB-WASM doesn't support ? parameters
            const pattern = `%${normalized.replace(/'/g, "''")}%`; // Escape single quotes
            const sql = `
                SELECT
                    lemma_id,
                    lemma,
                    language,
                    pos,
                    gloss_en,
                    frequency,
                    sahidic_form,
                    bohairic_form,
                    transliteration
                FROM lexicon.lemmas
                WHERE language = '${language}'
                  AND (
                    LOWER(lemma) LIKE '${pattern}'
                    OR LOWER(transliteration) LIKE '${pattern}'
                    OR LOWER(sahidic_form) LIKE '${pattern}'
                    OR LOWER(bohairic_form) LIKE '${pattern}'
                  )
                ORDER BY frequency DESC NULLS LAST
                LIMIT ${config.ui.autocompleteLimit}
            `;

            const results = await this.dbManager.query(sql);

            this.currentResults = results;
            this.showAutocomplete(results, language);
        } catch (error) {
            console.error('Search error:', error);
        }
    }

    showAutocomplete(results, language) {
        const suffix = this.getLanguageSuffix(language);
        const autocompleteDiv = document.getElementById(`autocomplete-${suffix}`);

        if (!autocompleteDiv) {
            console.error(`Autocomplete element not found: autocomplete-${suffix}`);
            return;
        }

        if (!results || results.length === 0) {
            this.hideAutocomplete(language);
            return;
        }

        autocompleteDiv.innerHTML = results.map((lemma, index) => {
            const displayForm = this.getDisplayForm(lemma);
            const gloss = lemma.gloss_en || '(no gloss)';
            const pos = lemma.pos || '';

            return `
                <div class="autocomplete-item" data-index="${index}" data-lemma-id="${lemma.lemma_id}">
                    <span class="lemma-form">${displayForm}</span>
                    ${pos ? `<span class="lemma-pos">[${pos}]</span>` : ''}
                    <span class="lemma-gloss">${gloss}</span>
                    ${lemma.frequency ? `<span class="lemma-freq">(${lemma.frequency.toLocaleString()})</span>` : ''}
                </div>
            `;
        }).join('');

        autocompleteDiv.classList.remove('hidden');
        this.selectedIndex = -1;

        // Add click listeners
        autocompleteDiv.querySelectorAll('.autocomplete-item').forEach(item => {
            item.addEventListener('click', () => {
                const lemmaId = item.dataset.lemmaId;
                this.selectLemma(lemmaId);
            });
        });
    }

    hideAutocomplete(language) {
        const suffix = this.getLanguageSuffix(language);
        const autocompleteDiv = document.getElementById(`autocomplete-${suffix}`);
        if (autocompleteDiv) {
            autocompleteDiv.classList.add('hidden');
            autocompleteDiv.innerHTML = '';
        }
    }

    handleKeyNavigation(e, language) {
        const suffix = this.getLanguageSuffix(language);
        const autocompleteDiv = document.getElementById(`autocomplete-${suffix}`);
        if (!autocompleteDiv) return;

        const items = autocompleteDiv.querySelectorAll('.autocomplete-item');
        if (items.length === 0) return;

        switch (e.key) {
            case 'ArrowDown':
                e.preventDefault();
                this.selectedIndex = Math.min(this.selectedIndex + 1, items.length - 1);
                this.updateSelection(items);
                break;

            case 'ArrowUp':
                e.preventDefault();
                this.selectedIndex = Math.max(this.selectedIndex - 1, -1);
                this.updateSelection(items);
                break;

            case 'Enter':
                e.preventDefault();
                if (this.selectedIndex >= 0 && items[this.selectedIndex]) {
                    const lemmaId = items[this.selectedIndex].dataset.lemmaId;
                    this.selectLemma(lemmaId);
                }
                break;

            case 'Escape':
                this.hideAutocomplete(language);
                break;
        }
    }

    updateSelection(items) {
        items.forEach((item, index) => {
            if (index === this.selectedIndex) {
                item.classList.add('selected');
                item.scrollIntoView({ block: 'nearest' });
            } else {
                item.classList.remove('selected');
            }
        });
    }

    async selectLemma(lemmaId) {
        // Hide all autocomplete dropdowns
        ['coptic', 'egyptian', 'greek'].forEach(lang => this.hideAutocomplete(lang));

        // Load lemma detail (will use LemmaDetail component)
        const { LemmaDetail } = await import('./lemma-detail.js');
        const detailContainer = document.getElementById('lemma-detail');
        detailContainer.classList.remove('hidden');

        const lemmaDetail = new LemmaDetail(detailContainer, this.dbManager);
        await lemmaDetail.loadLemma(lemmaId);
    }

    getDisplayForm(lemma) {
        // Choose the most appropriate display form based on language
        if (lemma.language === 'cop') {
            return lemma.sahidic_form || lemma.lemma;
        } else if (lemma.language === 'egy') {
            return lemma.transliteration || lemma.lemma;
        } else {
            return lemma.lemma;
        }
    }

    onNavigate(params) {
        // Handle navigation with lemma_id parameter
        if (params && params[0]) {
            this.selectLemma(params[0]);
        }
    }
}

export default Dictionary;

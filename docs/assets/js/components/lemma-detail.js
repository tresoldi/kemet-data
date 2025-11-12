/**
 * Lemma Detail Component
 * Displays comprehensive lemma information with split view
 */

import config from '../config.js';

// Helper function to escape SQL strings
function escapeSQL(str) {
    if (!str) return '';
    return String(str).replace(/'/g, "''");
}

export class LemmaDetail {
    constructor(container, dbManager) {
        this.container = container;
        this.dbManager = dbManager;
        this.currentLemma = null;
    }

    async loadLemma(lemmaId) {
        try {
            // Load main lemma data
            const lemmaData = await this.fetchLemmaData(lemmaId);
            if (!lemmaData) {
                this.showError('Lemma not found');
                return;
            }

            this.currentLemma = lemmaData;

            // Load related data sequentially to avoid overwhelming DuckDB-WASM worker
            // Running queries in parallel can cause "_setThrew is not defined" errors
            const forms = await this.fetchForms(lemmaId);
            const attestations = await this.fetchAttestations(lemmaId);
            const etymology = await this.fetchEtymology(lemmaId);

            // Concordance query can be problematic in DuckDB-WASM, so wrap in try-catch
            let concordance = [];
            try {
                concordance = await this.fetchConcordance(lemmaId, 5);
            } catch (error) {
                console.warn('Failed to fetch concordance:', error);
                // Continue without concordance data
            }

            this.render(lemmaData, forms, attestations, etymology, concordance);
        } catch (error) {
            console.error('Error loading lemma:', error);
            this.showError(`Failed to load lemma: ${error.message}`);
        }
    }

    async fetchLemmaData(lemmaId) {
        const sql = `
            SELECT
                lemma_id,
                lemma,
                language,
                script,
                period,
                pos,
                pos_detail,
                gloss_en,
                gloss_de,
                semantic_domain,
                semantic_field,
                hieroglyphic_writing,
                mdc_transcription,
                transliteration,
                bohairic_form,
                sahidic_form,
                frequency,
                document_count,
                collection_count,
                first_attested_date,
                last_attested_date,
                first_attested_period,
                last_attested_period,
                source,
                source_id
            FROM lexicon.lemmas
            WHERE lemma_id = '${escapeSQL(lemmaId)}'
        `;

        const results = await this.dbManager.query(sql);
        return results.length > 0 ? results[0] : null;
    }

    async fetchForms(lemmaId) {
        const sql = `
            SELECT
                form_id,
                form,
                morphology,
                frequency,
                relative_frequency
            FROM lexicon.forms
            WHERE lemma_id = '${escapeSQL(lemmaId)}'
            ORDER BY frequency DESC
            LIMIT 20
        `;

        return await this.dbManager.query(sql);
    }

    async fetchAttestations(lemmaId) {
        const sql = `
            SELECT
                dimension_type,
                dimension_value,
                frequency,
                document_count
            FROM lexicon.lemma_attestations
            WHERE lemma_id = '${escapeSQL(lemmaId)}'
            ORDER BY frequency DESC
            LIMIT 20
        `;

        return await this.dbManager.query(sql);
    }

    async fetchEtymology(lemmaId) {
        // Check if this is a Coptic lemma with Egyptian etymology
        const etymSql = `
            SELECT
                er.relation_id,
                er.relation_type,
                er.confidence,
                er.metadata,
                le.lemma as egyptian_lemma,
                le.gloss_en as egyptian_gloss,
                le.transliteration as egyptian_translit
            FROM lexicon.etymology_relations er
            LEFT JOIN lexicon.lemmas le ON er.target_lemma_id = le.lemma_id
            WHERE er.source_lemma_id = '${escapeSQL(lemmaId)}'
        `;

        return await this.dbManager.query(etymSql);
    }

    async fetchConcordance(lemmaId, limit = 5) {
        // Fetch token instances first (simple query)
        // Note: The table is called token_instances, not tokens
        const tokensSql = `
            SELECT
                token_id,
                segment_id,
                document_id,
                form,
                "order" as position_in_segment
            FROM corpus.token_instances
            WHERE lemma_id = '${escapeSQL(lemmaId)}'
            LIMIT ${limit}
        `;

        const tokens = await this.dbManager.query(tokensSql);
        if (tokens.length === 0) {
            return [];
        }

        // Fetch segment and document info for these tokens
        const segmentIds = tokens.map(t => `'${escapeSQL(t.segment_id)}'`).join(',');
        const contextSql = `
            SELECT
                s.segment_id,
                s.text as segment_text,
                d.title as document_title,
                d.document_id
            FROM corpus.segments s
            JOIN corpus.documents d ON s.document_id = d.document_id
            WHERE s.segment_id IN (${segmentIds})
        `;

        const contexts = await this.dbManager.query(contextSql);

        // Merge the results
        return tokens.map(token => {
            const context = contexts.find(c => c.segment_id === token.segment_id);
            return {
                token_id: token.token_id,
                segment_id: token.segment_id,
                lemma_id: lemmaId,
                form: token.form,
                position_in_segment: token.position_in_segment,
                segment_text: context?.segment_text || '',
                document_title: context?.document_title || '',
                document_id: context?.document_id || ''
            };
        });
    }

    render(lemma, forms, attestations, etymology, concordance) {
        const displayForm = this.getDisplayForm(lemma);
        const cdoId = this.getCDOId(lemma);

        this.container.innerHTML = `
            <div class="lemma-card">
                <!-- Header -->
                <div class="lemma-header">
                    <div class="lemma-title">
                        <h2 class="lemma-form ${this.getScriptClass(lemma.language)}">${displayForm}</h2>
                        ${lemma.language === 'egy' && lemma.hieroglyphic_writing ?
                            `<div class="hieroglyph-text">${lemma.hieroglyphic_writing}</div>` : ''}
                    </div>
                    <div class="lemma-meta-header">
                        <span class="badge">${this.getLanguageName(lemma.language)}</span>
                        ${lemma.pos ? `<span class="badge">${lemma.pos}</span>` : ''}
                        ${lemma.frequency ? `<span class="frequency-badge">${lemma.frequency.toLocaleString()} occurrences</span>` : ''}
                    </div>
                </div>

                <!-- Basic Information -->
                <div class="lemma-metadata">
                    ${lemma.gloss_en ? `
                        <div class="lemma-field">
                            <strong>English:</strong> ${lemma.gloss_en}
                        </div>
                    ` : ''}

                    ${lemma.gloss_de ? `
                        <div class="lemma-field">
                            <strong>German:</strong> ${lemma.gloss_de}
                        </div>
                    ` : ''}

                    ${lemma.language === 'egy' && lemma.transliteration ? `
                        <div class="lemma-field">
                            <strong>Transliteration:</strong> <span class="transliteration-text">${lemma.transliteration}</span>
                        </div>
                    ` : ''}

                    ${lemma.language === 'cop' && lemma.sahidic_form ? `
                        <div class="lemma-field">
                            <strong>Sahidic:</strong> <span class="coptic-text">${lemma.sahidic_form}</span>
                        </div>
                    ` : ''}

                    ${lemma.language === 'cop' && lemma.bohairic_form ? `
                        <div class="lemma-field">
                            <strong>Bohairic:</strong> <span class="coptic-text">${lemma.bohairic_form}</span>
                        </div>
                    ` : ''}

                    ${lemma.pos_detail ? `
                        <div class="lemma-field">
                            <strong>Part of Speech:</strong> ${lemma.pos_detail}
                        </div>
                    ` : ''}

                    ${lemma.semantic_domain ? `
                        <div class="lemma-field">
                            <strong>Semantic Domain:</strong> ${Array.isArray(lemma.semantic_domain) ? lemma.semantic_domain.join(', ') : lemma.semantic_domain}
                        </div>
                    ` : ''}

                    ${lemma.period ? `
                        <div class="lemma-field">
                            <strong>Period:</strong> ${lemma.period}
                        </div>
                    ` : ''}

                    ${lemma.first_attested_period || lemma.last_attested_period ? `
                        <div class="lemma-field">
                            <strong>Attestation Period:</strong> ${lemma.first_attested_period || '?'} → ${lemma.last_attested_period || '?'}
                        </div>
                    ` : ''}

                    ${lemma.document_count ? `
                        <div class="lemma-field">
                            <strong>Documents:</strong> ${lemma.document_count} ${lemma.collection_count ? `(${lemma.collection_count} collections)` : ''}
                        </div>
                    ` : ''}
                </div>

                <!-- External Links -->
                <div class="external-links">
                    ${lemma.source === 'tla' && lemma.source_id ? `
                        <a href="${config.externalLinks.tla(lemma.source_id)}" target="_blank">View in TLA →</a>
                    ` : ''}
                    ${cdoId ? `
                        <a href="${config.externalLinks.cdo(cdoId)}" target="_blank">View in CDO →</a>
                    ` : ''}
                </div>

                <!-- Split View: Forms and Morphology | Attestations and Statistics -->
                <div class="lemma-split-view">
                    <!-- Left: Forms -->
                    <div class="lemma-section">
                        <h3>Forms and Morphology</h3>
                        ${this.renderForms(forms, lemma.language)}
                    </div>

                    <!-- Right: Attestations -->
                    <div class="lemma-section">
                        <h3>Distribution</h3>
                        ${this.renderAttestations(attestations)}
                    </div>
                </div>

                <!-- Etymology Section (if Coptic) -->
                ${etymology && etymology.length > 0 ? `
                    <div class="lemma-section">
                        <h3>Etymology</h3>
                        ${this.renderEtymology(etymology)}
                    </div>
                ` : ''}

                <!-- Concordance Section -->
                ${concordance && concordance.length > 0 ? `
                    <div class="lemma-section">
                        <h3>Example Contexts</h3>
                        <p class="text-muted">Sample occurrences from the corpus (<a href="#" id="view-all-concordance">view all</a>)</p>
                        ${this.renderConcordance(concordance)}
                    </div>
                ` : ''}
            </div>
        `;

        // Attach event listener for "view all concordance"
        const viewAllLink = this.container.querySelector('#view-all-concordance');
        if (viewAllLink) {
            viewAllLink.addEventListener('click', (e) => {
                e.preventDefault();
                this.viewFullConcordance(lemma.lemma_id);
            });
        }
    }

    renderForms(forms, language) {
        if (!forms || forms.length === 0) {
            return '<p class="text-muted">No form data available</p>';
        }

        const scriptClass = this.getScriptClass(language);

        return `
            <table>
                <thead>
                    <tr>
                        <th>Form</th>
                        <th>Morphology</th>
                        <th>Frequency</th>
                        <th>%</th>
                    </tr>
                </thead>
                <tbody>
                    ${forms.map(form => `
                        <tr>
                            <td class="${scriptClass}">${form.form}</td>
                            <td class="text-muted">${form.morphology || '—'}</td>
                            <td>${form.frequency ? form.frequency.toLocaleString() : '—'}</td>
                            <td>${form.relative_frequency ? (form.relative_frequency * 100).toFixed(1) + '%' : '—'}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;
    }

    renderAttestations(attestations) {
        if (!attestations || attestations.length === 0) {
            return '<p class="text-muted">No attestation data available</p>';
        }

        // Group by dimension type
        const grouped = {};
        attestations.forEach(att => {
            if (!grouped[att.dimension_type]) {
                grouped[att.dimension_type] = [];
            }
            grouped[att.dimension_type].push(att);
        });

        return Object.entries(grouped).map(([type, items]) => `
            <div class="attestation-group">
                <h4>${type}</h4>
                <table>
                    <thead>
                        <tr>
                            <th>Value</th>
                            <th>Frequency</th>
                            <th>Documents</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${items.slice(0, 5).map(att => `
                            <tr>
                                <td>${att.dimension_value}</td>
                                <td>${att.frequency.toLocaleString()}</td>
                                <td>${att.document_count || '—'}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        `).join('');
    }

    renderEtymology(etymology) {
        return `
            <div class="etymology-list">
                ${etymology.map(etym => {
                    const metadata = typeof etym.metadata === 'string' ? JSON.parse(etym.metadata) : etym.metadata;
                    const oraecId = metadata?.oraec_id;
                    const tlaId = metadata?.tla_id;

                    return `
                        <div class="etymology-item">
                            ${etym.egyptian_lemma ? `
                                <p>← Egyptian: <strong class="transliteration-text">${etym.egyptian_translit || etym.egyptian_lemma}</strong>
                                ${etym.egyptian_gloss ? `"${etym.egyptian_gloss}"` : ''}</p>
                            ` : `
                                <p>← Egyptian lemma (unresolved)
                                ${oraecId ? `<a href="${config.externalLinks.oraec(oraecId)}" target="_blank">ORAEC ${oraecId}</a>` : ''}
                                ${tlaId ? `<a href="${config.externalLinks.tla(tlaId)}" target="_blank">TLA ${tlaId}</a>` : ''}
                                </p>
                            `}
                            ${etym.confidence ? `<span class="text-muted">Confidence: ${(etym.confidence * 100).toFixed(0)}%</span>` : ''}
                        </div>
                    `;
                }).join('')}
            </div>
        `;
    }

    renderConcordance(concordance) {
        return `
            <table class="kwic-table">
                <tbody>
                    ${concordance.map(ctx => {
                        const kwic = this.buildKWIC(ctx);
                        return `
                            <tr>
                                <td class="kwic-left">${kwic.left}</td>
                                <td class="kwic-keyword">${kwic.keyword}</td>
                                <td class="kwic-right">${kwic.right}</td>
                            </tr>
                        `;
                    }).join('')}
                </tbody>
            </table>
        `;
    }

    buildKWIC(context) {
        const text = context.segment_text || '';
        const form = context.form;
        const position = context.position_in_segment || 0;

        // Simple KWIC: split around the form
        // In a production system, you'd use the position to extract properly
        const index = text.indexOf(form);

        if (index >= 0) {
            const left = text.substring(0, index);
            const keyword = form;
            const right = text.substring(index + form.length);

            return {
                left: left.substring(Math.max(0, left.length - config.ui.concordanceContextWidth)),
                keyword,
                right: right.substring(0, config.ui.concordanceContextWidth)
            };
        }

        return {
            left: '',
            keyword: form,
            right: text.substring(0, config.ui.concordanceContextWidth)
        };
    }

    async viewFullConcordance(lemmaId) {
        // Navigate to concordance view with lemma filter
        window.location.hash = `#concordance/${lemmaId}`;
    }

    getDisplayForm(lemma) {
        if (lemma.language === 'cop') {
            return lemma.sahidic_form || lemma.lemma;
        } else if (lemma.language === 'egy') {
            return lemma.transliteration || lemma.lemma;
        } else {
            return lemma.lemma;
        }
    }

    getScriptClass(language) {
        const mapping = {
            'cop': 'coptic-text',
            'egy': 'transliteration-text',
            'grc': 'greek-text'
        };
        return mapping[language] || '';
    }

    getLanguageName(code) {
        const names = {
            'cop': 'Coptic',
            'egy': 'Egyptian',
            'grc': 'Greek',
            'he': 'Hebrew'
        };
        return names[code] || code;
    }

    getCDOId(lemma) {
        // Extract CDO ID from source_id if source is 'cdo'
        if (lemma.source === 'cdo' && lemma.source_id) {
            return lemma.source_id;
        }
        return null;
    }

    showError(message) {
        this.container.innerHTML = `
            <div class="error-message">
                ${message}
            </div>
        `;
    }
}

export default LemmaDetail;

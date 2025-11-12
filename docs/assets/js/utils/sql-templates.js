/**
 * SQL Query Templates
 * Parameterized queries for the Query Builder
 */

export const queryTemplates = {
    /**
     * 1. Concordance Query
     * Find all occurrences of a lemma with context
     */
    concordance: {
        id: 'concordance',
        name: 'Concordance (KWIC)',
        description: 'Find all occurrences of a lemma with surrounding context',
        category: 'corpus',
        parameters: [
            {
                name: 'lemma_id',
                label: 'Lemma ID',
                type: 'lemma_search',
                required: true,
                default: 'egy:lemma:=f',
                placeholder: 'e.g., cop:lemma:ⲁⲛⲟⲕ or egy:lemma:=f',
                helpText: 'Search for a lemma using the dictionary'
            },
            {
                name: 'collection',
                label: 'Collection (optional)',
                type: 'select',
                required: false,
                options: 'collections', // Will be populated from database
                allowNull: true
            },
            {
                name: 'dialect',
                label: 'Dialect (optional)',
                type: 'select',
                required: false,
                options: ['SAHIDIC', 'BOHAIRIC', 'AKHMIMIC', 'LYCOPOLITAN', 'FAYYUMIC'],
                allowNull: true
            },
            {
                name: 'limit',
                label: 'Maximum Results',
                type: 'number',
                required: true,
                default: 100,
                min: 1,
                max: 1000
            }
        ],
        buildSQL: (params) => {
            // Note: The table is called token_instances, not tokens
            let sql = `SELECT
    token_id,
    segment_id,
    form,
    lemma_id,
    lang
FROM corpus.token_instances
WHERE lemma_id = '${params.lemma_id}'
LIMIT ${params.limit || 100}`;

            return sql;
        }
    },

    /**
     * 2. Frequency Comparison
     * Compare lemma frequencies across collections or periods
     */
    frequencyComparison: {
        id: 'frequency_comparison',
        name: 'Frequency Comparison',
        description: 'Compare lemma frequencies across collections, dialects, or periods',
        category: 'lexicon',
        parameters: [
            {
                name: 'lemma_ids',
                label: 'Lemma IDs (comma-separated)',
                type: 'text',
                required: true,
                default: 'egy:lemma:=f, egy:lemma:=k',
                placeholder: 'e.g., egy:lemma:=f, egy:lemma:=k',
                helpText: 'Enter multiple lemma IDs separated by commas'
            },
            {
                name: 'dimension_type',
                label: 'Compare By',
                type: 'select',
                required: true,
                options: ['COLLECTION', 'DIALECT', 'PERIOD', 'STAGE', 'SUBSTAGE'],
                default: 'COLLECTION'
            },
            {
                name: 'min_frequency',
                label: 'Minimum Frequency',
                type: 'number',
                required: false,
                default: 1,
                min: 1
            }
        ],
        buildSQL: (params) => {
            const lemmaIds = params.lemma_ids.split(',').map(id => id.trim()).filter(id => id);
            const lemmaIdList = lemmaIds.map(id => `'${id}'`).join(', ');

            return `
SELECT
    l.lemma,
    l.language,
    l.gloss_en,
    la.dimension_value,
    la.frequency,
    la.document_count
FROM lexicon.lemma_attestations la
JOIN lexicon.lemmas l ON la.lemma_id = l.lemma_id
WHERE la.lemma_id IN (${lemmaIdList})
  AND la.dimension_type = '${params.dimension_type}'
  ${params.min_frequency ? `AND la.frequency >= ${params.min_frequency}` : ''}
ORDER BY la.dimension_value, la.frequency DESC;`;
        }
    },

    /**
     * 3. Form Variation
     * Analyze morphological variations of a lemma
     */
    formVariation: {
        id: 'form_variation',
        name: 'Form Variation Analysis',
        description: 'Analyze morphological forms and their frequencies for a lemma',
        category: 'lexicon',
        parameters: [
            {
                name: 'lemma_id',
                label: 'Lemma ID',
                type: 'lemma_search',
                required: true,
                default: 'egy:lemma:=f',
                placeholder: 'e.g., egy:lemma:=f or egy:lemma:m',
                helpText: 'Search for a lemma using the dictionary'
            },
            {
                name: 'min_frequency',
                label: 'Minimum Frequency',
                type: 'number',
                required: false,
                default: 1,
                min: 1
            },
            {
                name: 'show_morphology',
                label: 'Show Morphology Details',
                type: 'checkbox',
                required: false,
                default: true
            }
        ],
        buildSQL: (params) => {
            const columns = params.show_morphology
                ? `f.form,
    f.morphology,
    f.frequency,
    f.relative_frequency,
    ROUND(f.relative_frequency * 100, 2) as percentage`
                : `f.form,
    f.frequency,
    ROUND(f.relative_frequency * 100, 2) as percentage`;

            return `
SELECT
    ${columns}
FROM lexicon.forms f
WHERE f.lemma_id = '${params.lemma_id}'
  ${params.min_frequency ? `AND f.frequency >= ${params.min_frequency}` : ''}
ORDER BY f.frequency DESC;`;
        }
    },

    /**
     * 4. Attestation Timeline
     * Show lemma usage across time periods
     */
    attestationTimeline: {
        id: 'attestation_timeline',
        name: 'Attestation Timeline',
        description: 'Track lemma usage across historical periods',
        category: 'lexicon',
        parameters: [
            {
                name: 'lemma_id',
                label: 'Lemma ID',
                type: 'lemma_search',
                required: true,
                default: 'egy:lemma:m',
                placeholder: 'e.g., egy:lemma:m or egy:lemma:n',
                helpText: 'Search for a lemma using the dictionary'
            },
            {
                name: 'dimension_type',
                label: 'Time Granularity',
                type: 'select',
                required: true,
                options: ['PERIOD', 'STAGE', 'SUBSTAGE'],
                default: 'STAGE'
            }
        ],
        buildSQL: (params) => {
            return `
SELECT
    la.dimension_value as period,
    la.frequency,
    la.document_count,
    la.first_occurrence,
    la.last_occurrence
FROM lexicon.lemma_attestations la
WHERE la.lemma_id = '${params.lemma_id}'
  AND la.dimension_type = '${params.dimension_type}'
ORDER BY
    CASE
        -- Egyptian chronological order
        WHEN la.dimension_value = 'OLD_EGYPTIAN' THEN 1
        WHEN la.dimension_value = 'MIDDLE_EGYPTIAN' THEN 2
        WHEN la.dimension_value = 'LATE_EGYPTIAN' THEN 3
        WHEN la.dimension_value = 'DEMOTIC' THEN 4
        WHEN la.dimension_value = 'EGYPTIAN' THEN 5
        WHEN la.dimension_value = 'COPTIC' THEN 6
        ELSE 99
    END,
    la.frequency DESC;`;
        }
    },

    /**
     * 5. Lemma Search by Gloss
     * Find lemmas by English or German translation
     */
    glossSearch: {
        id: 'gloss_search',
        name: 'Search by Translation',
        description: 'Find lemmas by their English or German glosses',
        category: 'lexicon',
        parameters: [
            {
                name: 'search_term',
                label: 'Search Term',
                type: 'text',
                required: true,
                default: 'god',
                placeholder: 'e.g., life, death, god',
                helpText: 'Partial matches supported (case-insensitive)'
            },
            {
                name: 'language',
                label: 'Language',
                type: 'select',
                required: false,
                options: ['cop', 'egy', 'grc'],
                allowNull: true,
                labels: {
                    'cop': 'Coptic',
                    'egy': 'Egyptian',
                    'grc': 'Greek'
                }
            },
            {
                name: 'min_frequency',
                label: 'Minimum Frequency',
                type: 'number',
                required: false,
                default: 0,
                min: 0
            },
            {
                name: 'limit',
                label: 'Maximum Results',
                type: 'number',
                required: true,
                default: 50,
                min: 1,
                max: 500
            }
        ],
        buildSQL: (params) => {
            let sql = `
SELECT
    l.lemma_id,
    l.lemma,
    l.language,
    l.pos,
    l.gloss_en,
    l.gloss_de,
    l.transliteration,
    l.frequency
FROM lexicon.lemmas l
WHERE (
    LOWER(l.gloss_en) LIKE '%${params.search_term.toLowerCase()}%'
    OR LOWER(l.gloss_de) LIKE '%${params.search_term.toLowerCase()}%'
)`;

            if (params.language) {
                sql += `\n  AND l.language = '${params.language}'`;
            }

            if (params.min_frequency) {
                sql += `\n  AND l.frequency >= ${params.min_frequency}`;
            }

            sql += `\nORDER BY l.frequency DESC NULLS LAST
LIMIT ${params.limit || 50};`;

            return sql;
        }
    },

    /**
     * 6. Co-occurrence Analysis
     * Find lemmas that frequently appear together
     */
    cooccurrence: {
        id: 'cooccurrence',
        name: 'Co-occurrence Analysis',
        description: 'Find lemmas that frequently appear in the same segments',
        category: 'corpus',
        parameters: [
            {
                name: 'lemma_id',
                label: 'Target Lemma ID',
                type: 'lemma_search',
                required: true,
                default: 'egy:lemma:=f',
                placeholder: 'e.g., egy:lemma:=f or cop:lemma:ⲁⲛⲟⲕ',
                helpText: 'Find lemmas that co-occur with this lemma'
            },
            {
                name: 'min_cooccurrence',
                label: 'Minimum Co-occurrences',
                type: 'number',
                required: false,
                default: 5,
                min: 1
            },
            {
                name: 'limit',
                label: 'Maximum Results',
                type: 'number',
                required: true,
                default: 50,
                min: 1,
                max: 200
            }
        ],
        buildSQL: (params) => {
            // Note: Using simplified query to avoid DuckDB-WASM issues
            // CTEs and complex JOINs can cause "_setThrew" errors
            // This returns basic co-occurrence counts without full lemma metadata
            return `
SELECT
    lemma_id,
    COUNT(DISTINCT segment_id) as cooccurrence_count,
    COUNT(*) as token_count
FROM corpus.token_instances
WHERE segment_id IN (
    SELECT DISTINCT segment_id
    FROM corpus.token_instances
    WHERE lemma_id = '${params.lemma_id}'
)
AND lemma_id != '${params.lemma_id}'
AND lemma_id IS NOT NULL
GROUP BY lemma_id
HAVING COUNT(DISTINCT segment_id) >= ${params.min_cooccurrence || 5}
ORDER BY cooccurrence_count DESC
LIMIT ${params.limit || 50}`;
        }
    }
};

/**
 * Get all template categories
 */
export function getCategories() {
    const categories = new Set();
    Object.values(queryTemplates).forEach(template => {
        categories.add(template.category);
    });
    return Array.from(categories);
}

/**
 * Get templates by category
 */
export function getTemplatesByCategory(category) {
    return Object.values(queryTemplates).filter(t => t.category === category);
}

/**
 * Get template by ID
 */
export function getTemplate(templateId) {
    // Search by the id field, not the object key
    return Object.values(queryTemplates).find(t => t.id === templateId);
}

/**
 * Get all templates
 */
export function getAllTemplates() {
    return Object.values(queryTemplates);
}

export default {
    queryTemplates,
    getCategories,
    getTemplatesByCategory,
    getTemplate,
    getAllTemplates
};

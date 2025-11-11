/**
 * Query Cookbook
 * Example SQL queries demonstrating common use cases
 */

export const cookbookQueries = {
    // ========================================================================
    // CORPUS QUERIES
    // ========================================================================

    corpusStatistics: {
        id: 'corpus_statistics',
        name: 'Corpus Statistics',
        description: 'Get overall corpus statistics including document, segment, and token counts',
        category: 'corpus',
        database: 'corpus',
        sql: `SELECT
    COUNT(DISTINCT document_id) as total_documents,
    COUNT(DISTINCT segment_id) as total_segments,
    COUNT(*) as total_tokens
FROM corpus.tokens;`
    },

    collectionStatistics: {
        id: 'collection_statistics',
        name: 'Collection Statistics',
        description: 'Token counts per collection with document and segment counts',
        category: 'corpus',
        database: 'corpus',
        sql: `SELECT
    c.name as collection_name,
    c.collection_id,
    COUNT(DISTINCT d.document_id) as document_count,
    COUNT(DISTINCT s.segment_id) as segment_count,
    COUNT(t.token_id) as token_count
FROM corpus.collections c
JOIN corpus.documents d ON c.collection_id = d.collection_id
JOIN corpus.segments s ON d.document_id = s.document_id
JOIN corpus.tokens t ON s.segment_id = t.segment_id
GROUP BY c.collection_id, c.name
ORDER BY token_count DESC
LIMIT 15;`
    },

    sahidicDocuments: {
        id: 'sahidic_documents',
        name: 'Sahidic Coptic Documents',
        description: 'Find all documents in the Sahidic dialect',
        category: 'corpus',
        database: 'corpus',
        sql: `SELECT
    document_id,
    title,
    dialect,
    collection_id,
    token_count
FROM corpus.documents
WHERE dialect = 'SAHIDIC'
ORDER BY token_count DESC
LIMIT 20;`
    },

    concordanceExample: {
        id: 'concordance_example',
        name: 'Concordance: ⲛⲟⲩⲧⲉ (god)',
        description: 'KWIC concordance for the Coptic word "ⲛⲟⲩⲧⲉ" (god)',
        category: 'corpus',
        database: 'corpus',
        sql: `SELECT
    t.form,
    t.lemma_id,
    t.morphology,
    s.text as segment_text,
    d.title as document_title,
    d.dialect
FROM corpus.tokens t
JOIN corpus.segments s ON t.segment_id = s.segment_id
JOIN corpus.documents d ON s.document_id = d.document_id
WHERE t.lemma_id = 'cop:lemma:ⲛⲟⲩⲧⲉ'
ORDER BY d.document_id, s.sequence
LIMIT 50;`
    },

    posDistribution: {
        id: 'pos_distribution',
        name: 'Part-of-Speech Distribution',
        description: 'Token counts by POS tag (cross-database query)',
        category: 'corpus',
        database: 'corpus+lexicon',
        sql: `SELECT
    l.pos,
    COUNT(*) as token_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM corpus.tokens t
JOIN lexicon.lemmas l ON t.lemma_id = l.lemma_id
WHERE l.pos IS NOT NULL
GROUP BY l.pos
ORDER BY token_count DESC
LIMIT 15;`
    },

    // ========================================================================
    // LEXICON QUERIES
    // ========================================================================

    dictionaryLookup: {
        id: 'dictionary_lookup',
        name: 'Dictionary Lookup: ⲁⲛⲟⲕ (I, me)',
        description: 'Look up a specific Coptic lemma',
        category: 'lexicon',
        database: 'lexicon',
        sql: `SELECT
    lemma_id,
    lemma,
    pos,
    gloss_en,
    gloss_de,
    frequency,
    document_count,
    sahidic_form,
    bohairic_form
FROM lexicon.lemmas
WHERE lemma = 'ⲁⲛⲟⲕ'
  AND language = 'cop';`
    },

    frequencyList: {
        id: 'frequency_list',
        name: 'Top 50 Most Frequent Lemmas',
        description: 'Frequency-ranked list of all lemmas',
        category: 'lexicon',
        database: 'lexicon',
        sql: `SELECT
    lemma,
    language,
    pos,
    gloss_en,
    frequency,
    document_count,
    CASE
        WHEN language = 'cop' THEN sahidic_form
        WHEN language = 'egy' THEN transliteration
        ELSE lemma
    END as display_form
FROM lexicon.lemmas
WHERE frequency IS NOT NULL
ORDER BY frequency DESC
LIMIT 50;`
    },

    copticVerbs: {
        id: 'coptic_verbs',
        name: 'Top Coptic Verbs',
        description: 'Most frequent Coptic verbs with dialectal forms',
        category: 'lexicon',
        database: 'lexicon',
        sql: `SELECT
    lemma,
    gloss_en,
    frequency,
    sahidic_form,
    bohairic_form,
    document_count
FROM lexicon.lemmas
WHERE language = 'cop'
  AND pos = 'VERB'
  AND frequency IS NOT NULL
ORDER BY frequency DESC
LIMIT 20;`
    },

    formMapping: {
        id: 'form_mapping',
        name: 'Form to Lemma: ⲡ',
        description: 'Find all possible lemmas for the surface form "ⲡ"',
        category: 'lexicon',
        database: 'lexicon',
        sql: `SELECT DISTINCT
    l.lemma_id,
    l.lemma,
    l.pos,
    l.gloss_en,
    f.morphology,
    f.frequency as form_frequency
FROM lexicon.forms f
JOIN lexicon.lemmas l ON f.lemma_id = l.lemma_id
WHERE f.form = 'ⲡ'
ORDER BY f.frequency DESC;`
    },

    morphologicalForms: {
        id: 'morphological_forms',
        name: 'Morphological Forms: ⲉⲓⲙⲉ (know)',
        description: 'All attested forms of a lemma with morphology',
        category: 'lexicon',
        database: 'lexicon',
        sql: `SELECT
    form,
    morphology,
    frequency,
    relative_frequency,
    ROUND(relative_frequency * 100, 2) as percentage
FROM lexicon.forms
WHERE lemma_id = 'cop:lemma:ⲉⲓⲙⲉ'
ORDER BY frequency DESC
LIMIT 20;`
    },

    collectionDiversity: {
        id: 'collection_diversity',
        name: 'Collection Diversity',
        description: 'Lemmas attested across multiple collections',
        category: 'lexicon',
        database: 'lexicon',
        sql: `SELECT
    l.lemma,
    l.pos,
    l.gloss_en,
    COUNT(DISTINCT la.dimension_value) as collection_count,
    l.frequency
FROM lexicon.lemmas l
JOIN lexicon.lemma_attestations la ON l.lemma_id = la.lemma_id
WHERE la.dimension_type = 'COLLECTION'
  AND l.language = 'cop'
GROUP BY l.lemma_id, l.lemma, l.pos, l.gloss_en, l.frequency
HAVING COUNT(DISTINCT la.dimension_value) > 5
ORDER BY collection_count DESC, l.frequency DESC
LIMIT 20;`
    },

    attestationsByPeriod: {
        id: 'attestations_by_period',
        name: 'Attestations by Period: nṯr (god)',
        description: 'Track lemma usage across Egyptian historical periods',
        category: 'lexicon',
        database: 'lexicon',
        sql: `SELECT
    dimension_value as period,
    frequency,
    document_count
FROM lexicon.lemma_attestations
WHERE lemma_id = 'egy:lemma:nṯr'
  AND dimension_type = 'STAGE'
ORDER BY frequency DESC;`
    },

    // ========================================================================
    // ETYMOLOGY AND CROSS-REFERENCES
    // ========================================================================

    etymologyExample: {
        id: 'etymology_example',
        name: 'Etymology: ⲕⲁϩ (earth)',
        description: 'Look up Coptic etymology from Egyptian',
        category: 'etymology',
        database: 'lexicon',
        sql: `SELECT
    lc.lemma as coptic_lemma,
    lc.gloss_en as coptic_gloss,
    er.relation_type,
    er.confidence,
    json_extract_string(er.metadata, '$.cdo_id') as cdo_id,
    json_extract_string(er.metadata, '$.oraec_id') as oraec_id,
    json_extract_string(er.metadata, '$.tla_id') as tla_id
FROM lexicon.lemmas lc
JOIN lexicon.etymology_relations er ON lc.lemma_id = er.source_lemma_id
WHERE lc.lemma = 'ⲕⲁϩ';`
    },

    frequentEtymologies: {
        id: 'frequent_etymologies',
        name: 'Frequent Lemmas with Etymologies',
        description: 'Top frequent Coptic lemmas with Egyptian etymology data',
        category: 'etymology',
        database: 'lexicon',
        sql: `SELECT
    l.lemma,
    l.gloss_en,
    l.frequency,
    COUNT(er.relation_id) as etymology_count
FROM lexicon.lemmas l
JOIN lexicon.etymology_relations er ON l.lemma_id = er.source_lemma_id
WHERE l.language = 'cop'
GROUP BY l.lemma_id, l.lemma, l.gloss_en, l.frequency
ORDER BY l.frequency DESC
LIMIT 20;`
    },

    cdoLookup: {
        id: 'cdo_lookup',
        name: 'CDO Cross-Reference',
        description: 'Look up Coptic Dictionary Online ID mappings',
        category: 'etymology',
        database: 'lexicon',
        sql: `SELECT
    cdo.cdo_id,
    cdo.lemma,
    l.gloss_en,
    l.pos,
    l.frequency,
    l.sahidic_form,
    l.bohairic_form
FROM lexicon.cdo_mappings cdo
JOIN lexicon.lemmas l ON cdo.lemma_id = l.lemma_id
ORDER BY l.frequency DESC
LIMIT 20;`
    },

    tlaHieroglyphs: {
        id: 'tla_hieroglyphs',
        name: 'TLA Egyptian Lemmas with Hieroglyphs',
        description: 'Top Egyptian lemmas with hieroglyphic writing',
        category: 'etymology',
        database: 'lexicon',
        sql: `SELECT
    lemma_id,
    transliteration,
    hieroglyphic_writing,
    gloss_en,
    frequency,
    source_id as tla_id
FROM lexicon.lemmas
WHERE language = 'egy'
  AND source = 'tla'
  AND hieroglyphic_writing IS NOT NULL
  AND frequency > 1000
ORDER BY frequency DESC
LIMIT 20;`
    },

    // ========================================================================
    // ADVANCED QUERIES
    // ========================================================================

    searchByGloss: {
        id: 'search_by_gloss',
        name: 'Search by Translation: "life"',
        description: 'Find lemmas by English gloss (semantic search)',
        category: 'advanced',
        database: 'lexicon',
        sql: `SELECT
    lemma,
    language,
    pos,
    gloss_en,
    frequency,
    CASE
        WHEN language = 'cop' THEN sahidic_form
        WHEN language = 'egy' THEN transliteration
        ELSE lemma
    END as display_form
FROM lexicon.lemmas
WHERE LOWER(gloss_en) LIKE '%life%'
  OR LOWER(gloss_de) LIKE '%leben%'
ORDER BY frequency DESC NULLS LAST
LIMIT 30;`
    },

    tokenWithFullInfo: {
        id: 'token_full_info',
        name: 'Token with Full Lemma Info',
        description: 'Join tokens with complete lemma information',
        category: 'advanced',
        database: 'corpus+lexicon',
        sql: `SELECT
    t.token_id,
    t.form,
    t.morphology,
    l.lemma,
    l.pos,
    l.gloss_en,
    d.title,
    s.text as segment_text
FROM corpus.tokens t
JOIN lexicon.lemmas l ON t.lemma_id = l.lemma_id
JOIN corpus.segments s ON t.segment_id = s.segment_id
JOIN corpus.documents d ON s.document_id = d.document_id
WHERE l.language = 'cop'
  AND l.pos = 'VERB'
  AND d.dialect = 'SAHIDIC'
LIMIT 50;`
    }
};

/**
 * Get all query categories
 */
export function getCategories() {
    const categories = new Set();
    Object.values(cookbookQueries).forEach(query => {
        categories.add(query.category);
    });
    return Array.from(categories);
}

/**
 * Get queries by category
 */
export function getQueriesByCategory(category) {
    return Object.values(cookbookQueries).filter(q => q.category === category);
}

/**
 * Get query by ID
 */
export function getQuery(queryId) {
    return cookbookQueries[queryId];
}

/**
 * Get all queries
 */
export function getAllQueries() {
    return Object.values(cookbookQueries);
}

/**
 * Get category display name
 */
export function getCategoryName(category) {
    const names = {
        'corpus': 'Corpus Queries',
        'lexicon': 'Lexicon Queries',
        'etymology': 'Etymology & Cross-References',
        'advanced': 'Advanced Queries'
    };
    return names[category] || category;
}

export default {
    cookbookQueries,
    getCategories,
    getQueriesByCategory,
    getQuery,
    getAllQueries,
    getCategoryName
};

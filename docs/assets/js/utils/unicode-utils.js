/**
 * Unicode and Script Detection Utilities
 */

export function detectScript(text) {
    if (!text) return 'unknown';

    // Coptic Unicode range: U+2C80-U+2CFF
    if (/[\u2C80-\u2CFF]/.test(text)) {
        return 'coptic';
    }

    // Hieroglyph Unicode range: U+13000-U+1342F
    if (/[\u{13000}-\u{1342F}]/u.test(text)) {
        return 'hieroglyph';
    }

    // Greek Unicode range: U+0370-U+03FF
    if (/[\u0370-\u03FF]/.test(text)) {
        return 'greek';
    }

    // Latin transliteration (for Egyptian)
    if (/^[a-zA-Z0-9ḥḫẖḏḍṯṭꜣꜥśšḳḵ.=]+$/.test(text)) {
        return 'transliteration';
    }

    return 'latin';
}

export function getLanguageFromScript(script) {
    const mapping = {
        'coptic': 'cop',
        'hieroglyph': 'egy',
        'transliteration': 'egy',
        'greek': 'grc',
        'latin': null
    };
    return mapping[script];
}

export function normalizeForSearch(text) {
    // Trim and lowercase
    return text.trim().toLowerCase();
}

export default {
    detectScript,
    getLanguageFromScript,
    normalizeForSearch
};

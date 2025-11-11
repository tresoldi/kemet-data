# KEMET Data - TODO

**Last Updated**: 2025-12-01
**Current Version**: 0.1.0

---

## Pre-Release Checklist (v0.1.0)

**Target Release**: 2025-12-01

### Essential Tasks
- [x] Integrate all planned data sources (6 sources: Horner, Scriptorium, UD-Coptic, AES, Ramses, TLA)
- [x] Update all documentation with current statistics
- [x] Verify all SQL examples in DATABASE.md
- [x] Run full test suite (18/18 passing)
- [x] Clean up development artifacts (logs, PROJECT_STATUS.md)
- [x] Update LICENSE file with accurate dates and source attributions

### Before Zenodo Upload
- [x] Improve code quality checks (ruff, mypy) - All auto-fixable issues resolved, tests passing
- [x] Review all documentation files for accuracy and completeness - Statistics verified against database
- [x] Generate Zenodo package - Package created in dist/zenodo/ (724MB)
- [x] Test package integrity - All databases validated, queries tested

### After Zenodo Upload
- [ ] Upload to Zenodo and obtain DOI
- [ ] Update README.md with actual DOI
- [ ] Update LICENSE with actual DOI
- [ ] Tag release in git repository

---

## Completed in v0.1.0

- ✅ **TLA Hieroglyphs Integration**: Loaded 16,379 hieroglyphic segments (12,773 earlier_egyptian + 3,606 late_egyptian) and 29,762 German translations into `segments` table
- ✅ **Coptic Etymological Dictionary**: Integrated ORAEC digitized etymologies with 1,750 Coptic→Egyptian relationships, 9,306 CDO lemmas imported, and 9,493 CDO ID cross-references
- ✅ **8 Data Sources**: Horner, Scriptorium, UD-Coptic, AES, Ramses, TLA, CDO, ORAEC

---

## Future Development (Post-v0.1.0)

### Data Expansion
- [ ] **Egyptian etymology resolution**: Resolve ORAEC/TLA IDs to KEMET lemma_ids for full etymology graph
  - Data sources identified: ORAEC corpus_raw_data (13K+ texts), TLA HuggingFace datasets (55K+ lemmas)
  - Key finding: AED/TLA/ORAEC use unified ID system
  - Approach: Extract lemmaID → lemma_form mappings from ORAEC JSON files + TLA datasets
  - License: CC-BY-SA-4.0 / CC0 (fully compatible)
  - Estimated effort: 1.5-2 days implementation
  - Expected resolution: 70-80% via direct lookup, 20-30% via fuzzy matching
  - See: docs/ETYMOLOGY_RESOLUTION_PLAN.md (to be created)
- [ ] **Additional Egyptian sources**: Explore other Late Egyptian/Demotic corpora
- [ ] **Greek loanwords**: Enhanced cross-linguistic metadata for Greek terms in Coptic

### Code Quality & Performance
- [ ] **KISS/DRY review**: Simplify architecture, reduce duplication
- [x] **Type checking**: Full mypy compliance with strict mode (37 source files, 0 errors)
- [x] **Linting**: Comprehensive ruff configuration (306 issues fixed, 0 errors)
- [ ] **Performance optimization**: Profile and optimize slow queries
- [ ] **Database indexing**: Review and optimize database indices
- [ ] **Build performance**: Optimize lexicon build process (currently ~37 min)

### Features & Usability
- [ ] **Query API**: Python interface for common database queries
- [ ] **Full-text search**: DuckDB FTS integration for corpus exploration
- [ ] **Export utilities**: CSV/JSON exports for lexicon and frequency lists
- [ ] **Web interface**: Browser-based corpus exploration tool
- [ ] **NLP integrations**: Export to Hugging Face Datasets, SpaCy, CONLL-U formats

### Research Features
- [ ] **Cross-linguistic alignment**: Parallel text alignment (e.g., Coptic NT ↔ Greek NT)
- [ ] **Advanced analytics**: Frequency distributions, collocation networks, dialectal variation
- [ ] **Semantic relations**: Integrate TLA semantic data when available

### Documentation
- [ ] **Query cookbook**: Collection of common query patterns with examples
- [ ] **Data quality report**: Detailed analysis of known limitations per source
- [ ] **API documentation**: If query API is developed
- [ ] **Tutorial notebooks**: Jupyter notebooks demonstrating common use cases

---

## Notes

- **Version 0.1.0 Focus**: Data completeness, accuracy, reproducibility
- **Future versions**: Enhanced features, additional sources, performance optimization
- **Community input**: Suggestions welcome via project repository

---

**Remember**: v0.1.0 is a foundational release focused on providing high-quality, well-documented data. Future versions will expand functionality and data coverage.

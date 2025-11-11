#!/usr/bin/env python3
"""Fetch detailed TLA data from SRU API for matched lemmas.

This script queries the TLA API for lemmas that were matched in Phase 1
and extracts etymology and semantic relationship data.

API: https://textplus.thesaurus-linguae-aegyptiae.de/sru/
Rate limit: Max 250 records per request
"""

from pathlib import Path
from typing import Dict, Optional
import logging
import json
import time
import xml.etree.ElementTree as ET

import requests


class TLAAPIFetcher:
    """Fetch detailed TLA data from SRU API."""

    BASE_URL = "https://textplus.thesaurus-linguae-aegyptiae.de/sru/"
    MAX_RECORDS = 250  # API limit
    DELAY_BETWEEN_REQUESTS = 1.0  # seconds

    # XML namespaces
    NS = {
        'sru': 'http://docs.oasis-open.org/ns/search-ws/sruResponse',
        'fcs': 'http://clarin.eu/fcs/resource',
        'lex': 'http://clarin.eu/fcs/dataview/lex',
        'hits': 'http://clarin.eu/fcs/dataview/hits'
    }

    def __init__(
        self,
        cache_dir: Path,
        logger: Optional[logging.Logger] = None
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logger or logging.getLogger(__name__)

    def load_matched_lemmas(self, matches_path: Path) -> Dict[str, Dict]:
        """Load TLA lemma matches from Phase 1."""
        self.logger.info(f"Loading matched lemmas from {matches_path}")

        with matches_path.open() as f:
            matches = json.load(f)

        # Filter to only matched lemmas
        matched = {
            tla_id: data
            for tla_id, data in matches.items()
            if data.get('match_type') == 'exact'
        }

        self.logger.info(f"Loaded {len(matched)} matched lemmas")
        return matched

    def fetch_by_transliteration(self, transliteration: str) -> Optional[str]:
        """Fetch TLA records by lemma transliteration.

        Args:
            transliteration: Lemma transliteration to search for

        Returns:
            XML response as string, or None on error
        """
        params = {
            'operation': 'searchRetrieve',
            'version': '2.0',
            'query': transliteration,
            'maximumRecords': self.MAX_RECORDS,
            'x-fcs-dataviews': 'lex'  # Request lexical data view
        }

        try:
            self.logger.debug(f"Fetching records for '{transliteration}'")
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            return None

    def parse_entry(self, record_elem) -> Optional[Dict]:
        """Parse a single TLA entry from XML.

        Extracts:
        - Entry ID
        - Lemma (transliteration + hieroglyphs)
        - Translations
        - POS
        - Baseform (etymology link)
        - Related lemmas (semantic relations)
        """
        entry = record_elem.find('.//lex:Entry', self.NS)
        if entry is None:
            return None

        data = {}

        # Extract fields
        for field in entry.findall('lex:Field', self.NS):
            field_type = field.get('type')
            values = []

            for value in field.findall('lex:Value', self.NS):
                val_text = value.text
                lang = value.get('{http://www.w3.org/XML/1998/namespace}lang')
                ref = value.get('ref')
                preferred = value.get('preferred')

                val_info = {
                    'text': val_text,
                    'lang': lang,
                    'ref': ref,
                    'preferred': preferred == 'true' if preferred else None
                }
                values.append(val_info)

            data[field_type] = values

        # Extract entry ID
        entry_id = None
        if 'entryId' in data and data['entryId']:
            entry_id = data['entryId'][0]['text']

        # Extract lemma
        lemma_translit = None
        lemma_hieroglyphs = None
        if 'lemma' in data:
            for val in data['lemma']:
                if val.get('preferred'):
                    lemma_translit = val['text']
                elif val.get('lang') == 'egy-Egyp':
                    lemma_hieroglyphs = val['text']

        # Extract translations
        translations = {}
        if 'translation' in data:
            for val in data['translation']:
                lang = val.get('lang')
                if lang:
                    translations[lang] = val['text']

        # Extract POS
        pos = None
        if 'pos' in data:
            for val in data['pos']:
                if val.get('preferred'):
                    pos = val['text']
                    break

        # Extract baseform (etymology)
        baseform = None
        baseform_ref = None
        if 'baseform' in data and data['baseform']:
            baseform = data['baseform'][0]['text']
            baseform_ref = data['baseform'][0]['ref']

        # Extract related lemmas (semantic relations)
        related = []
        if 'related' in data:
            for val in data['related']:
                related.append({
                    'lemma': val['text'],
                    'ref': val['ref'],
                    'lang': val.get('lang')
                })

        return {
            'tla_id': entry_id,
            'transliteration': lemma_translit,
            'hieroglyphs': lemma_hieroglyphs,
            'translations': translations,
            'pos': pos,
            'baseform': baseform,
            'baseform_ref': baseform_ref,
            'related_lemmas': related
        }

    def fetch_all_lemmas(
        self,
        matched_lemmas: Dict[str, Dict],
        resume_from: int = 0
    ) -> Dict[str, Dict]:
        """Fetch detailed data for all matched lemmas.

        Strategy: Query by unique transliterations since ID-based queries don't work.
        We'll get all entries for each transliteration and match back to TLA IDs.

        Args:
            matched_lemmas: Dict of TLA ID -> match data from Phase 1
            resume_from: Resume from this index (for interrupted runs)

        Returns:
            Dict of TLA ID -> detailed API data
        """
        # Group by transliteration
        translit_to_tla_ids = {}
        for tla_id, data in matched_lemmas.items():
            translit = data['transliteration']
            if translit not in translit_to_tla_ids:
                translit_to_tla_ids[translit] = []
            translit_to_tla_ids[translit].append(tla_id)

        unique_translits = list(translit_to_tla_ids.keys())
        total = len(unique_translits)

        self.logger.info(f"Fetching detailed data for {total} unique transliterations")
        self.logger.info(f"  (covering {len(matched_lemmas)} total lemmas)")
        if resume_from > 0:
            self.logger.info(f"Resuming from index {resume_from}")

        detailed_data = {}
        fetched_count = 0

        # Process one transliteration at a time (API searches are lemma-based)
        for i in range(resume_from, total):
            translit = unique_translits[i]
            expected_tla_ids = translit_to_tla_ids[translit]

            if (i + 1) % 100 == 0:
                self.logger.info(
                    f"Progress: {i+1}/{total} transliterations "
                    f"({fetched_count} lemmas fetched)"
                )

            # Fetch by transliteration
            xml_response = self.fetch_by_transliteration(translit)
            if xml_response is None:
                self.logger.warning(f"Failed to fetch '{translit}', skipping")
                continue

            # Parse response
            try:
                root = ET.fromstring(xml_response)
                records = root.findall('.//sru:record', self.NS)

                self.logger.debug(f"Received {len(records)} records for '{translit}'")

                for record in records:
                    entry_data = self.parse_entry(record)
                    if entry_data and entry_data['tla_id']:
                        # Only save if this TLA ID is in our matched list
                        if entry_data['tla_id'] in expected_tla_ids:
                            detailed_data[entry_data['tla_id']] = entry_data
                            fetched_count += 1

            except ET.ParseError as e:
                self.logger.error(f"Failed to parse XML for '{translit}': {e}")
                continue

            # Save incremental cache every 100 transliterations
            if (i + 1) % 100 == 0:
                batch_num = (i + 1) // 100
                self.save_cache(detailed_data, suffix=f"_batch{batch_num}")

            # Rate limiting
            if i + 1 < total:
                time.sleep(self.DELAY_BETWEEN_REQUESTS)

        self.logger.info(f"Fetched detailed data for {len(detailed_data)} lemmas")
        return detailed_data

    def save_cache(self, data: Dict, suffix: str = ""):
        """Save fetched data to cache."""
        cache_path = self.cache_dir / f"tla_api_data{suffix}.json"

        with cache_path.open('w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        self.logger.debug(f"Saved cache to {cache_path}")

    def load_cache(self, suffix: str = "") -> Optional[Dict]:
        """Load cached data if available."""
        cache_path = self.cache_dir / f"tla_api_data{suffix}.json"

        if cache_path.exists():
            with cache_path.open(encoding='utf-8') as f:
                return json.load(f)
        return None


def main():
    """Run TLA API fetcher."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    # Paths
    cache_dir = Path.home() / ".cache" / "kemet" / "tla"
    matches_path = cache_dir / "tla_lemma_matches.json"

    if not matches_path.exists():
        logger.error(f"Matches file not found: {matches_path}")
        logger.error("Run tla_huggingface.py first")
        exit(1)

    # Create fetcher
    fetcher = TLAAPIFetcher(cache_dir=cache_dir, logger=logger)

    # Load matched lemmas
    matched_lemmas = fetcher.load_matched_lemmas(matches_path)

    # Count unique transliterations
    unique_translits = set(data['transliteration'] for data in matched_lemmas.values())
    logger.info(f"Total matched lemmas: {len(matched_lemmas)}")
    logger.info(f"Unique transliterations: {len(unique_translits)}")
    logger.info(f"Estimated API calls needed: ~{len(unique_translits)}")
    logger.info(f"Estimated time (1s/call): ~{len(unique_translits)/60:.1f} minutes")

    # Check for existing cache
    existing_cache = fetcher.load_cache()
    resume_from = 0
    if existing_cache:
        logger.info(f"Found existing cache with {len(existing_cache)} entries")
        logger.info("Will merge with existing cache")

    # Fetch all lemmas
    detailed_data = fetcher.fetch_all_lemmas(matched_lemmas, resume_from=resume_from)

    # Save final cache
    fetcher.save_cache(detailed_data)

    # Print summary
    print("\n=== TLA API Fetch Complete ===")
    print(f"Total lemmas fetched: {len(detailed_data)}")

    # Count lemmas with etymology/relations
    with_baseform = sum(1 for d in detailed_data.values() if d.get('baseform'))
    with_related = sum(1 for d in detailed_data.values() if d.get('related_lemmas'))

    print(f"Lemmas with baseform (etymology): {with_baseform}")
    print(f"Lemmas with related lemmas: {with_related}")


if __name__ == "__main__":
    main()

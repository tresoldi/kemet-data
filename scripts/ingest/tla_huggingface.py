#!/usr/bin/env python3
"""Download and parse TLA data from Hugging Face dataset.

This script downloads the TLA (Thesaurus Linguae Aegyptiae) Earlier Egyptian
corpus from Hugging Face and extracts lemma information for integration into
the KEMET lexicon database.

Dataset: thesaurus-linguae-aegyptiae/tla-Earlier_Egyptian_original-v18-premium
"""

import json
import logging
from pathlib import Path
from typing import Any


try:
    from datasets import load_dataset
except ImportError:
    print("ERROR: datasets library not installed")
    print("Install with: pip install datasets")
    exit(1)

import duckdb


class TLAHuggingFaceIngester:
    """Download and parse TLA data from Hugging Face."""

    def __init__(
        self,
        lexicon_db_path: Path,
        cache_dir: Path | None = None,
        logger: logging.Logger | None = None
    ) -> None:
        self.lexicon_db_path = Path(lexicon_db_path)
        self.cache_dir = cache_dir or Path.home() / ".cache" / "kemet" / "tla"
        self.logger = logger or logging.getLogger(__name__)

    def download_dataset(self) -> Any:
        """Download TLA dataset from Hugging Face."""
        self.logger.info("Downloading TLA dataset from Hugging Face...")

        # Load dataset
        dataset = load_dataset(
            "thesaurus-linguae-aegyptiae/tla-Earlier_Egyptian_original-v18-premium",
            split="train",
            cache_dir=str(self.cache_dir)
        )

        # Convert to pandas
        df: Any = dataset.to_pandas()

        self.logger.info(f"Downloaded {len(df)} sentences from TLA")
        return df

    def extract_lemmas(self, df: Any) -> dict[str, dict[str, Any]]:
        """Extract unique TLA lemmas from dataset.

        Lemmatization format: "TLA_ID|transliteration TLA_ID|transliteration ..."
        Example: "90880|nḏ 51510|wdi̯ 91901|r 10090|=s"

        Returns:
            Dict mapping TLA_ID -> {transliteration, hieroglyphs, count}
        """
        self.logger.info("Extracting TLA lemmas from dataset...")

        lemmas: dict[str, dict[str, Any]] = {}

        for _idx, row in df.iterrows():
            lemmatization = row.get('lemmatization', '')
            hieroglyphs = row.get('hieroglyphs', '')

            if not lemmatization:
                continue

            # Parse lemmatization field
            # Format: "TLA_ID|transliteration TLA_ID|transliteration ..."
            tokens = lemmatization.split()
            hieroglyph_tokens = hieroglyphs.split() if hieroglyphs else []

            for i, token in enumerate(tokens):
                if '|' not in token:
                    continue

                tla_id, translit = token.split('|', 1)

                # Skip clitics (=) and grammatical markers
                if translit.startswith('='):
                    continue

                # Get corresponding hieroglyph token if available
                hieroglyph = hieroglyph_tokens[i] if i < len(hieroglyph_tokens) else None

                if tla_id not in lemmas:
                    lemmas[tla_id] = {
                        'tla_id': tla_id,
                        'transliteration': translit,
                        'hieroglyphs': hieroglyph,
                        'attestation_count': 0
                    }

                lemmas[tla_id]['attestation_count'] += 1

        self.logger.info(f"Extracted {len(lemmas)} unique TLA lemmas")
        return lemmas

    def match_to_corpus(self, tla_lemmas: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Match TLA lemmas to existing corpus lemmas.

        Returns:
            Dict mapping TLA_ID -> {corpus_lemma_id, match_type, ...}
        """
        self.logger.info("Matching TLA lemmas to corpus lemmas...")

        # Connect to lexicon database
        conn = duckdb.connect(str(self.lexicon_db_path), read_only=True)

        # Get all Egyptian lemmas from corpus
        corpus_lemmas = conn.execute("""
            SELECT
                lemma_id,
                lemma,
                language,
                pos
            FROM lemmas
            WHERE language = 'egy'
        """).fetchdf()

        conn.close()

        self.logger.info(f"Found {len(corpus_lemmas)} Egyptian lemmas in corpus")

        # Create mapping by transliteration
        corpus_by_translit: dict[str, list[dict[str, Any]]] = {}
        for _idx, row in corpus_lemmas.iterrows():
            translit = row['lemma']
            lemma_id = row['lemma_id']

            if translit not in corpus_by_translit:
                corpus_by_translit[translit] = []
            corpus_by_translit[translit].append({
                'lemma_id': lemma_id,
                'pos': row['pos']
            })

        # Match TLA lemmas to corpus
        matches = {}
        match_stats = {'exact': 0, 'no_match': 0}

        for tla_id, tla_data in tla_lemmas.items():
            translit = tla_data['transliteration']

            if translit in corpus_by_translit:
                # Exact match found
                corpus_matches = corpus_by_translit[translit]

                # If multiple matches, prefer first one (could be enhanced with POS matching)
                best_match = corpus_matches[0]

                matches[tla_id] = {
                    **tla_data,
                    'lemma_id': best_match['lemma_id'],
                    'match_type': 'exact',
                    'num_corpus_matches': len(corpus_matches)
                }
                match_stats['exact'] += 1
            else:
                # No match
                matches[tla_id] = {
                    **tla_data,
                    'lemma_id': None,
                    'match_type': 'no_match',
                    'num_corpus_matches': 0
                }
                match_stats['no_match'] += 1

        self.logger.info(f"Match statistics: {match_stats}")
        self.logger.info(f"  Exact matches: {match_stats['exact']} ({match_stats['exact']/len(tla_lemmas)*100:.1f}%)")
        self.logger.info(f"  No matches: {match_stats['no_match']} ({match_stats['no_match']/len(tla_lemmas)*100:.1f}%)")

        return matches

    def save_to_cache(self, matches: dict[str, dict[str, Any]], output_path: Path | None = None) -> None:
        """Save TLA lemma matches to JSON cache file."""
        if output_path is None:
            output_path = self.cache_dir / "tla_lemma_matches.json"

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with output_path.open('w', encoding='utf-8') as f:
            json.dump(matches, f, ensure_ascii=False, indent=2)

        self.logger.info(f"Saved TLA lemma matches to {output_path}")

    def run(self) -> dict[str, dict[str, Any]]:
        """Run complete TLA ingestion pipeline."""
        # Download dataset
        df = self.download_dataset()

        # Extract lemmas
        tla_lemmas = self.extract_lemmas(df)

        # Match to corpus
        matches = self.match_to_corpus(tla_lemmas)

        # Save to cache
        self.save_to_cache(matches)

        return matches


def main() -> None:
    """Run TLA Hugging Face ingestion."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Paths
    project_root = Path(__file__).parent.parent.parent
    lexicon_db_path = project_root / "data" / "derived" / "lexicon.duckdb"

    # Run ingestion
    ingester = TLAHuggingFaceIngester(lexicon_db_path=lexicon_db_path)
    matches = ingester.run()

    print("\n=== TLA Ingestion Complete ===")
    print(f"Total TLA lemmas: {len(matches)}")
    print(f"Matched to corpus: {sum(1 for m in matches.values() if m['match_type'] == 'exact')}")
    print(f"Not matched: {sum(1 for m in matches.values() if m['match_type'] == 'no_match')}")


if __name__ == "__main__":
    main()

"""Jaccard similarity logic, shared by the pipeline and the API."""
import json
import random
import re
import zlib

MERSENNE_PRIME = (1 << 61) - 1

STOPWORDS = {
    "the","a","an","and","or","of","in","on","to","for","is","are","was",
    "were","be","been","being","this","that","these","those","with","as",
    "by","at","from","it","its","we","our","which","also","can","such",
    "using","based","paper","results","show","study","propose","present",
    "between","into","than","then","not","no","have","has","had","will",
    "would","may","could","should","their","they","them","he","she","his",
    "her","i","you","your","if","but","so","up","out","about","over",
    "under","new","two","one","three","used","method","methods","approach",
    "data","however","where","when","what","how","proposed","novel","framework",
    "experiments","experimental","demonstrate","achieve","performance","improve",
    "improved","compared","existing","different","various","several",
}


class TextProcessor:
    """Raw JSON line -> parsed record; abstract -> set of k-word shingles."""

    def __init__(self, k, stopwords=STOPWORDS):
        self.k = k
        self.stopwords = stopwords

    @staticmethod
    def parse_record(line):
        """JSON line -> (paper_id, title, abstract) or None if malformed, too short or withdrawn."""
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            return None
        paper_id = rec.get("id")
        title = (rec.get("title") or "").strip()
        abstract = (rec.get("abstract") or "").strip()
        if not paper_id or len(abstract) < 20:
            return None
        if "withdrawn" in abstract.lower() and len(abstract) < 300:  # "This paper has been withdrawn..."
            return None
        return (paper_id, title, abstract)

    @staticmethod
    def clean_text(text):
        text = re.sub(r"[^a-z\s]", " ", text.lower())
        return re.sub(r"\s+", " ", text).strip()

    def tokenize(self, text):
        return [t for t in text.split() if t not in self.stopwords and len(t) > 2]

    def shingles(self, abstract):
        """k-word shingles, e.g. ['a','b','c','d'], k=3 -> {'a b c', 'b c d'}"""
        tokens = self.tokenize(self.clean_text(abstract))
        k = self.k
        if len(tokens) < k:
            return {" ".join(tokens)} if tokens else set()
        return {" ".join(tokens[i:i + k]) for i in range(len(tokens) - k + 1)}


class MinHasher:
    """Shingle set -> fixed-length signature; P(positions match) ≈ Jaccard."""

    def __init__(self, num_hashes, seed, prime=MERSENNE_PRIME):
        rng = random.Random(seed)
        self.prime = prime
        # h(x) = (a * x + b) % prime  -- same sequence as the notebook
        self.coeffs = [(rng.randint(1, prime - 1), rng.randint(0, prime - 1))
                       for _ in range(num_hashes)]

    @staticmethod
    def stable_hash(shingle):
        """Deterministic 32-bit CRC32 (built-in hash() is not stable across executors)."""
        return zlib.crc32(shingle.encode("utf-8"))

    def signature(self, shingles):
        p = self.prime
        if not shingles:
            return tuple([p] * len(self.coeffs))
        ints = [self.stable_hash(s) for s in shingles]
        return tuple(min((a * x + b) % p for x in ints) for a, b in self.coeffs)


class LSH:
    """Signature -> bands; papers sharing a band become candidate pairs."""

    def __init__(self, num_bands, rows_per_band):
        self.num_bands = num_bands
        self.rows_per_band = rows_per_band

    def bands(self, signature):
        """One signature -> [(band_index, band_key), ...]"""
        r = self.rows_per_band
        return [(b, self.band_key(signature[b * r:(b + 1) * r])) for b in range(self.num_bands)]

    @staticmethod
    def band_key(values):
        """Band values -> string key; stored in SQL so an API (C# or Python) can rebuild it exactly."""
        return "-".join(str(v) for v in values)

    @staticmethod
    def pairs_in_bucket(ids):
        ids = sorted(set(ids))
        return [(ids[i], ids[j]) for i in range(len(ids)) for j in range(i + 1, len(ids))]


def jaccard_similarity(set_a, set_b):
    """|A ∩ B| / |A ∪ B|"""
    union = len(set_a | set_b)
    return len(set_a & set_b) / union if union else 0.0

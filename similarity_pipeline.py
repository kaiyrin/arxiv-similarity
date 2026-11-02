"""
arXiv similar abstracts: shingling -> MinHash -> LSH -> exact Jaccard (PySpark).
Results are saved to SQL Server so the C# by API to read
"""
import json
import os
import random
import re
import zlib
from dataclasses import dataclass, field

import pyodbc
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

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

def _env(name, default) -> str:
    return os.environ.get(name) or default

@dataclass(frozen=True)
class Config:
    data_path: str = field(default_factory=lambda: _env("DATA_PATH", "data/arxiv-metadata-oai-snapshot.json"))
    sample_fraction: float = field(default_factory=lambda: float(_env("SAMPLE_FRACTION", "0.05")))
    shingle_size: int = 3          # k-word shingles
    num_hashes: int = 100          # MinHash permutation functions
    num_bands: int = field(default_factory=lambda: int(_env("NUM_BANDS", "20")))  # must divide num_hashes
    jaccard_threshold: float = field(default_factory=lambda: float(_env("JACCARD_THRESHOLD", "0.3")))
    min_shingles: int = 5          # drop near-empty abstracts (caused false 1.000 matches)
    random_seed: int = 42

    def __post_init__(self):
        assert self.num_hashes % self.num_bands == 0, (
            f"num_hashes ({self.num_hashes}) must be evenly divisible by num_bands ({self.num_bands})"
        )

    @property
    def rows_per_band(self):
        return self.num_hashes // self.num_bands


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
        """Band values -> string key; stored in SQL so C# can rebuild it exactly."""
        return "-".join(str(v) for v in values)

    @staticmethod
    def pairs_in_bucket(ids):
        ids = sorted(set(ids))
        return [(ids[i], ids[j]) for i in range(len(ids)) for j in range(i + 1, len(ids))]


def jaccard_similarity(set_a, set_b):
    """|A ∩ B| / |A ∪ B|"""
    union = len(set_a | set_b)
    return len(set_a & set_b) / union if union else 0.0


PAPERS_SCHEMA = StructType([
    StructField("PaperId", StringType(), False),
    StructField("Title", StringType(), False),
    StructField("Abstract", StringType(), False),
    StructField("ShingleCount", IntegerType(), False),
])
PAIRS_SCHEMA = StructType([
    StructField("PaperAId", StringType(), False),
    StructField("PaperBId", StringType(), False),
    StructField("Jaccard", DoubleType(), False),
])
BANDS_SCHEMA = StructType([
    StructField("BandIndex", IntegerType(), False),
    StructField("BandKey", StringType(), False),
    StructField("PaperId", StringType(), False),
])
COEFFS_SCHEMA = StructType([
    StructField("HashIndex", IntegerType(), False),
    StructField("A", LongType(), False),   # < 2^61, fits BIGINT
    StructField("B", LongType(), False),
])


@dataclass
class PipelineResult:
    papers: DataFrame
    similar_pairs: DataFrame
    band_keys: DataFrame
    hash_coeffs: DataFrame


class SimilarityPipeline:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.text = TextProcessor(config.shingle_size)
        self.minhash = MinHasher(config.num_hashes, config.random_seed)
        self.lsh = LSH(config.num_bands, config.rows_per_band)

    def run(self):
        cfg = self.config
        # Local copies: Spark lambdas must not capture `self` (it holds the SparkSession).
        text, minhash, lsh = self.text, self.minhash, self.lsh
        min_shingles, threshold = cfg.min_shingles, cfg.jaccard_threshold

        # 1-2. Load, sample, parse, shingle
        # 3. MinHash -> (paper_id, title, abstract, shingles, signature)
        docs = (self.spark.sparkContext.textFile(cfg.data_path)
                .sample(withReplacement=False, fraction=cfg.sample_fraction, seed=cfg.random_seed)
                .map(TextProcessor.parse_record)
                .filter(lambda r: r is not None)
                .map(lambda r: (r[0], r[1], r[2], text.shingles(r[2])))
                .filter(lambda r: len(r[3]) >= min_shingles)
                .map(lambda r: r + (minhash.signature(r[3]),))
                .cache())

        # 4. LSH: ((band_index, band_key), paper_id) -> buckets -> candidate pairs
        band_rows = docs.flatMap(lambda r: [(band, r[0]) for band in lsh.bands(r[4])]).cache()
        candidates = (band_rows
                      .groupByKey()
                      .mapValues(list)
                      .filter(lambda kv: len(kv[1]) > 1)          # drop singleton buckets
                      .flatMap(lambda kv: LSH.pairs_in_bucket(kv[1]))
                      .distinct())

        # 5. Exact Jaccard on candidates only (joins instead of collecting to the driver)
        shingles_by_id = docs.map(lambda r: (r[0], r[3]))
        similar = (candidates
                   .join(shingles_by_id)                          # (a, (b, sh_a))
                   .map(lambda x: (x[1][0], (x[0], x[1][1])))     # (b, (a, sh_a))
                   .join(shingles_by_id)                          # (b, ((a, sh_a), sh_b))
                   .map(lambda x: (x[1][0][0], x[0], jaccard_similarity(x[1][0][1], x[1][1])))
                   .filter(lambda x: x[2] >= threshold)
                   .cache())

        return PipelineResult(
            papers=self.spark.createDataFrame(
                docs.map(lambda r: (r[0], r[1], r[2], len(r[3]))), PAPERS_SCHEMA),
            similar_pairs=self.spark.createDataFrame(similar, PAIRS_SCHEMA),
            band_keys=self.spark.createDataFrame(
                band_rows.map(lambda x: (x[0][0], x[0][1], x[1])), BANDS_SCHEMA),
            hash_coeffs=self.spark.createDataFrame(
                [(i, a, b) for i, (a, b) in enumerate(minhash.coeffs)], COEFFS_SCHEMA),
        )


class SqlServerWriter:
    """Creates the database + tables (pyodbc) and bulk-writes DataFrames (Spark JDBC)."""

    DROP_TABLES = [
        "DROP TABLE IF EXISTS dbo.SimilarPairs",
        "DROP TABLE IF EXISTS dbo.BandKeys",
        "DROP TABLE IF EXISTS dbo.HashCoeffs",
        "DROP TABLE IF EXISTS dbo.Papers",
    ]
    CREATE_TABLES = [
        """CREATE TABLE dbo.Papers (
               PaperId      NVARCHAR(50)   NOT NULL PRIMARY KEY,
               Title        NVARCHAR(1000) NOT NULL,
               Abstract     NVARCHAR(MAX)  NOT NULL,
               ShingleCount INT            NOT NULL)""",
        """CREATE TABLE dbo.SimilarPairs (
               Id       INT IDENTITY(1,1) PRIMARY KEY,
               PaperAId NVARCHAR(50) NOT NULL REFERENCES dbo.Papers(PaperId),
               PaperBId NVARCHAR(50) NOT NULL REFERENCES dbo.Papers(PaperId),
               Jaccard  FLOAT        NOT NULL)""",
        "CREATE INDEX IX_SimilarPairs_PaperAId ON dbo.SimilarPairs(PaperAId)",
        "CREATE INDEX IX_SimilarPairs_PaperBId ON dbo.SimilarPairs(PaperBId)",
        """CREATE TABLE dbo.BandKeys (
               BandIndex INT          NOT NULL,
               BandKey   VARCHAR(120) NOT NULL,
               PaperId   NVARCHAR(50) NOT NULL REFERENCES dbo.Papers(PaperId),
               PRIMARY KEY (BandIndex, BandKey, PaperId))""",
        """CREATE TABLE dbo.HashCoeffs (
               HashIndex INT    NOT NULL PRIMARY KEY,
               A         BIGINT NOT NULL,
               B         BIGINT NOT NULL)""",
    ]

    def __init__(self, host, port, database, user, password):
        self.database = database
        self.user = user
        self.password = password
        self.jdbc_url = (f"jdbc:sqlserver://{host}:{port};databaseName={database};"
                         "encrypt=true;trustServerCertificate=true")
        self._odbc = ("DRIVER={ODBC Driver 18 for SQL Server};"
                      f"SERVER={host},{port};UID={user};PWD={{{password}}};"
                      "Encrypt=yes;TrustServerCertificate=yes;")


    @classmethod
    def from_env(cls):
        return cls(
            host=os.environ.get("SQL_HOST", "localhost"),
            port=os.environ.get("SQL_PORT", "1433"),
            database=os.environ.get("SQL_DATABASE", "ArxivSimilarity"),
            user=os.environ["SQL_USER"],
            password=os.environ["SQL_PASSWORD"],
        )

    def reset_schema(self):
        """Create the database if missing, then drop + recreate all tables."""
        with pyodbc.connect(self._odbc + "DATABASE=master;", autocommit=True) as conn:
            conn.execute(f"IF DB_ID('{self.database}') IS NULL CREATE DATABASE [{self.database}]")
        with pyodbc.connect(self._odbc + f"DATABASE={self.database};", autocommit=True) as conn:
            for sql in self.DROP_TABLES + self.CREATE_TABLES:
                conn.execute(sql)

    def write(self, df, table):
        (df.write.format("jdbc")
           .option("url", self.jdbc_url)
           .option("dbtable", table)
           .option("user", self.user)
           .option("password", self.password)
           .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
           .option("batchsize", 10000)
           .mode("append")                      # tables already created with proper types/keys
           .save())

    def save(self, result):
        self.write(result.papers, "dbo.Papers")  # first: other tables reference it
        self.write(result.similar_pairs, "dbo.SimilarPairs")
        self.write(result.band_keys, "dbo.BandKeys")
        self.write(result.hash_coeffs, "dbo.HashCoeffs")


def create_spark_session(app_name="ArxivSimilarAbstracts"):
    spark = (SparkSession.builder
             .appName(app_name)
             .master("local[*]")
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
             .config("spark.sql.shuffle.partitions", "16")
             .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    load_dotenv()
    config = Config()
    db = SqlServerWriter.from_env()
    spark = create_spark_session()
    try:
        result = SimilarityPipeline(spark, config).run()
        print(f"Papers: {result.papers.count():,}")
        print(f"Similar pairs (>= {config.jaccard_threshold}): {result.similar_pairs.count():,}")
        db.reset_schema()
        db.save(result)
        print(f"Saved to SQL Server database '{db.database}'")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
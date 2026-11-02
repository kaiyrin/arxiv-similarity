"""FastAPI service: query precomputed similar pairs and match new abstracts via LSH."""
import os

import pyodbc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from similarity_core import LSH, MinHasher, TextProcessor, jaccard_similarity

CONN = ("DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.environ.get('SQL_HOST', 'localhost')},{os.environ.get('SQL_PORT', '1433')};"
        f"DATABASE={os.environ.get('SQL_DATABASE', 'ArxivSimilarity')};"
        f"UID={os.environ['SQL_USER']};PWD={{{os.environ['SQL_PASSWORD']}}};"
        "Encrypt=yes;TrustServerCertificate=yes;")

app = FastAPI(title="arXiv Similar Abstracts")
text = TextProcessor(k=3)
NUM_BANDS = int(os.environ.get("NUM_BANDS", "20"))


def db():
    return pyodbc.connect(CONN)


def clean(title):
    """Collapse line breaks / repeated spaces in arXiv titles."""
    return " ".join(title.split())


def load_minhasher():
    """Rebuild the exact MinHash functions the pipeline used, from HashCoeffs."""
    with db() as conn:
        rows = conn.execute("SELECT A, B FROM dbo.HashCoeffs ORDER BY HashIndex").fetchall()
    mh = MinHasher(num_hashes=len(rows), seed=0)
    mh.coeffs = [(r.A, r.B) for r in rows]
    return mh


minhash = load_minhasher()
lsh = LSH(NUM_BANDS, len(minhash.coeffs) // NUM_BANDS)


class AbstractIn(BaseModel):
    abstract: str
    threshold: float = 0.3


class Match(BaseModel):
    paper_id: str
    title: str
    jaccard: float


class Paper(BaseModel):
    paper_id: str
    title: str
    abstract: str


@app.get("/papers/{paper_id}", response_model=Paper)
def get_paper(paper_id: str):
    with db() as conn:
        row = conn.execute(
            "SELECT PaperId, Title, Abstract FROM dbo.Papers WHERE PaperId = ?", paper_id
        ).fetchone()
    if row is None:
        raise HTTPException(404, "Paper not found")
    return {"paper_id": row.PaperId, "title": clean(row.Title), "abstract": row.Abstract}


@app.get("/papers/{paper_id}/similar", response_model=list[Match])
def similar_to_paper(paper_id: str):
    sql = """SELECT p.PaperBId AS Id, b.Title, p.Jaccard FROM dbo.SimilarPairs p
             JOIN dbo.Papers b ON b.PaperId = p.PaperBId WHERE p.PaperAId = ?
             UNION ALL
             SELECT p.PaperAId, a.Title, p.Jaccard FROM dbo.SimilarPairs p
             JOIN dbo.Papers a ON a.PaperId = p.PaperAId WHERE p.PaperBId = ?
             ORDER BY Jaccard DESC"""
    with db() as conn:
        rows = conn.execute(sql, paper_id, paper_id).fetchall()
    return [{"paper_id": r.Id, "title": clean(r.Title), "jaccard": round(r.Jaccard, 3)}
            for r in rows]


@app.post("/similar", response_model=list[Match])
def similar_to_new_abstract(body: AbstractIn):
    shingles = text.shingles(body.abstract)
    if len(shingles) < 5:
        raise HTTPException(400, "Abstract too short")
    bands = lsh.bands(minhash.signature(shingles))

    # LSH lookup: papers sharing at least one band with the new abstract
    where = " OR ".join("(BandIndex = ? AND BandKey = ?)" for _ in bands)
    params = [v for b in bands for v in b]
    with db() as conn:
        candidates = conn.execute(
            f"""SELECT p.PaperId, p.Title, p.Abstract FROM dbo.Papers p
                WHERE p.PaperId IN (SELECT DISTINCT PaperId FROM dbo.BandKeys WHERE {where})""",
            *params).fetchall()

    # exact Jaccard only on candidates
    results = [{"paper_id": c.PaperId, "title": clean(c.Title),
                "jaccard": round(jaccard_similarity(shingles, text.shingles(c.Abstract)), 3)}
               for c in candidates]
    return sorted([r for r in results if r["jaccard"] >= body.threshold],
                  key=lambda r: -r["jaccard"])

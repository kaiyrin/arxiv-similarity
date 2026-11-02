# Similar arXiv Abstracts: Shingling → MinHash → LSH → Jaccard

Project for the Algorithms class. Finds pairs of arXiv papers with similar abstracts
and stores the results in SQL Server.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/kaiyrin/similarity_september26/blob/main/jaccard_sim.ipynb)

**Just want to see the algorithm?** Open the Colab notebook above. It runs in the browser, no setup.
**Just want to see results?** See [`results/query_results.txt`](results/query_results.txt).
**Full write-up:** [report.pdf](report.pdf) · **Plots:** [plots/](plots/) (MinHash, LSH S-curve, Jaccard distributions)


## How it works
1. **Shingling**: each abstract is cleaned and turned into a set of 3-word shingles.
2. **MinHash**: each set becomes a 100-number signature (matching positions ≈ Jaccard similarity).
3. **LSH**: signatures are split into bands; papers that share a band become candidate pairs.
4. **Exact Jaccard**: only candidates are compared; pairs ≥ 0.3 are kept.
5. **SQL Server**: papers, similar pairs, band keys and hash coefficients are saved in 4 tables.

## Run it yourself (Docker)

**You only need [Docker Desktop].**
Java, Spark, Python and SQL Server all run inside containers.

```bash
git clone https://github.com/kaiyrin/similarity_september26.git
cd similarity_september26

# 1. Create your config (edit the password if you like)
cp .env.example .env

# 2. Start SQL Server and run the pipeline (first run downloads images, ~5 min)
docker compose run --rm --build pipeline

# 3. Query the results
docker compose exec sqlserver sh -c '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -d ArxivSimilarity -W -i /sql/queries.sql'
```

The pipeline prints the number of papers and similar pairs, then `Saved to SQL Server database 'ArxivSimilarity'`.

**Password rules:** SQL Server will not start unless `SQL_PASSWORD` has 8+ characters and
3 of: uppercase, lowercase, digit, symbol. Avoid `$` and `}` in it.

You can also connect any SQL client (Azure Data Studio, DBeaver, VS Code) to
`localhost,1433`, user `sa`, password from `.env`, database `ArxivSimilarity`.

### Tables
| Table | Contents |
|---|---|
| `Papers` | PaperId, Title, Abstract, ShingleCount |
| `SimilarPairs` | PaperAId, PaperBId, Jaccard (≥ 0.3) |
| `BandKeys` | LSH bucket of every paper per band |
| `HashCoeffs` | MinHash coefficients (a, b) to rebuild signatures |

## Try the API ‎(skip if you already did step 2 above)

```bash
docker compose run --rm --build pipeline   # 1. fill the database (once)
docker compose up -d --build api           # 2. start the API
```

Open **http://localhost:8000/docs** in your browser. There's an interactive page
where you can click any endpoint → "Try it out" → "Execute".

| Endpoint | Example |
|---|---|
| `GET /papers/{id}` | full title and abstract of `0704.0003` |
| `GET /papers/{id}/similar` | papers similar to `0708.0098` |
| `POST /similar` | paste any abstract and get matching papers |

Or from the terminal:
```bash
curl http://localhost:8000/papers/0708.0098/similar      # precomputed pairs
curl http://localhost:8000/papers/0704.0003              # get a full abstract
```
Copy the `abstract` from the second call into `POST /similar` on `/docs`.
The paper should find itself with `jaccard: 1.0`.

Stop everything: `docker compose down`


## Full dataset (optional)
The repo includes a small sample in `sample/`. To use the full arXiv metadata:
1. Download `arxiv-metadata-oai-snapshot.json` from the Kaggle dataset *Cornell-University/arxiv*.
2. Put it in `data/`.
3. In `.env` set `DATA_PATH=data/arxiv-metadata-oai-snapshot.json`, `SAMPLE_FRACTION=0.05`,
   `SPARK_DRIVER_MEMORY=4g`, and give Docker Desktop at least 6 GB of memory.
4. Run step 2 again.

## Settings (`.env`)
| Variable | Default | Meaning |
|---|---|---|
| `SAMPLE_FRACTION` | 1.0 (sample) | Fraction of papers to use |
| `NUM_BANDS` | 20 | LSH bands (must divide 100). 20×5 starts catching pairs around 0.55 similarity; 50×2 catches ~99% of pairs at 0.3 |
| `JACCARD_THRESHOLD` | 0.3 | Minimum similarity to keep a pair |

## Reset / clean up
```bash
docker compose down -v   # stops containers and deletes the database
```

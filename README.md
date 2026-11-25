# Data Processing Lab

**Data Engineering 1 · SRH Hochschule Heidelberg**  
Prof. Dr. Ing. Binh Vu & Sumit Kulhadia

---

## Contents

| File | Description |
|---|---|
| `Data_Processing.html` | Interactive study guide with diagrams, concept comparisons, and the full hands-on lab |
| `data_processing_setup.sh` | One-shot setup script — installs everything and writes all lab files |

---

## Getting Started (3 commands)

```bash
# 1. Clone this repository
git clone <REPO_URL>
cd data-processing

# 2. Run the setup script
bash data_processing_setup.sh

# 3. Activate the environment and run the lab
source ~/data-processing-lab/venv/activate
python3 ~/data-processing-lab/data_processing_lab.py
```

> **Requirement:** Ubuntu 22.04 (or any Debian-based Linux).  
> The script handles Python, the virtual environment, all libraries, sample data generation, and writes every lab file automatically.

---

## What the Setup Script Does

1. Installs `python3`, `pip`, `venv`, `sqlite3` via `apt`
2. Creates `~/data-processing-lab/` with a Python virtual environment
3. Installs: `pandas`, `petl`
4. Generates reproducible sample datasets (`bookings.csv`, `countries_code.csv`, `stream_events.csv`)
5. Writes all lab Python files into `~/data-processing-lab/`
6. Runs `check_setup.py` to verify everything before you start

---

## Lab Structure

| Phase | Task | Slide Reference |
|---|---|---|
| 1 | Verify setup (`check_setup.py`) | — |
| 2 | Batch ETL pipeline with Pandas | Slides 5, 20, 24 |
| 3 | Stream processing — tumbling, hopping, session windows | Slides 9–13 |
| 4 | ELT pattern — SQL transformations inside the warehouse | Slides 21–23 |
| **Full** | **Complete pipeline — assignment deliverable** | **All** |

Open `Data_Processing.html` in your browser for the full interactive guide.

---

## Running Individual Phases

```bash
cd ~/data-processing-lab
source venv/bin/activate

python3 check_setup.py              # Phase 1 — verify environment
python3 phase2_batch_etl.py         # Phase 2 — Batch ETL (Pandas)
python3 phase3_stream_windows.py    # Phase 3 — Stream windows
python3 phase4_elt.py               # Phase 4 — ELT (SQL in SQLite)
python3 data_processing_lab.py      # Full pipeline — deliverable
```

---

## Concepts Covered

| Concept | Description |
|---|---|
| **Batch Processing** | ETL pipeline: Extract CSV → Transform with Pandas → Load to SQLite |
| **Stream Processing** | Windowing simulation: Tumbling, Hopping, and Session windows |
| **ETL Pattern** | Transform on staging server, load only clean data into the warehouse |
| **ELT Pattern** | Load raw data first, transform inside the warehouse using SQL |

# Self-Healing Agentic Data Pipeline Simulator

This project simulates a self-healing data pipeline using a simplified subset of the Brazilian Olist e-commerce dataset.

The canonical source is a local SQLite database, not a synthetic stream generator. Each simulation cycle samples clean records from that database, injects random data-quality failures, validates them, attempts AI-style auto-healing, and escalates unresolved issues to a human queue.

## What it demonstrates

- real-dataset-inspired pipeline inputs: `orders`, `payments`, `delivery`
- random dirty-data failures on every run
- validation, diagnosis, auto-healing, and escalation
- a visual dashboard that updates on a 15-second cycle
- audit logs and per-cycle evidence for presentation/demo use

## Project structure

- `/Users/garv/Documents/New project/server.py`
- `/Users/garv/Documents/New project/scripts/prepare_olist_subset.py`
- `/Users/garv/Documents/New project/web/index.html`
- `/Users/garv/Documents/New project/web/styles.css`
- `/Users/garv/Documents/New project/web/app.js`
- `/Users/garv/Documents/New project/data/standard.db`
- `/Users/garv/Documents/New project/data/clean`
- `/Users/garv/Documents/New project/data/runs`

## Run it

```bash
python3 scripts/prepare_olist_subset.py
python3 server.py
```

Then open [http://localhost:8000](http://localhost:8000).

## Use the real Olist dataset

Place these files in `/Users/garv/Documents/New project/data/source`:

- `olist_orders_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_items_dataset.csv`

Then run:

```bash
python3 scripts/prepare_olist_subset.py --rows 200
```

The script will:

1. extract a manageable subset
2. build simplified `orders`, `payments`, and `delivery` tables
3. write clean CSVs
4. populate `/Users/garv/Documents/New project/data/standard.db`

## If the real CSVs are not present

The preparation script falls back to a deterministic demo seed with Olist-like columns so the simulator still runs locally.

## Random failure types included

- missing column
- null primary key
- duplicate rows
- invalid payment value type
- orphan payment or delivery record
- malformed timestamp
- suspicious freight value
- unknown payment type

## Demo flow

Every 15 seconds the dashboard:

1. samples clean records from the standard database
2. injects random failures
3. runs validation
4. performs auto-healing where possible
5. escalates unresolved issues
6. updates the audit log and visuals

## Good presentation line

“We used a reduced subset of a real Brazilian e-commerce dataset as our standard source database, then deliberately introduced random pipeline failures during each simulation cycle to test self-healing behavior.”

## Research & Benchmarking Suite

This project includes a fully automated research benchmarking suite to generate publication-ready insights representing the system’s performance. 

You can extract and download these charts right from the dashboard:
1. Open the UI to the **Analytics** view.
2. Scroll to the **Research & Benchmarking Suite** panel.
3. Click **Generate Research Report** to spin up headless benchmarking sessions against the `SimulationEngine`.
4. Once completed, view or download the fully rendered HTML report, underlying metrics JSON, and publication-ready SVG charts (Radar, Dual-Bar, Heatmap, Pie).

### Configuring State-of-the-Art Baselines
When generating metrics (Precision, Recall, MTTD, MTTR), the pipeline compares the performance of your LLM self-healing system against pre-defined baselines. 
You can substitute these baselines to match cited literature values by modifying `research_baselines.json` located in the root of the project.

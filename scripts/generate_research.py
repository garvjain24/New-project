import json
import random
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from engine import SimulationEngine
from config import BASE_DIR, CLEAN_DIR
from pipeline_utils import load_dataset, ERROR_TYPES
import research_utils as ru

def main():
    print("Initializing SimulationEngine...")
    engine = SimulationEngine()
    engine.state["running"] = False # Pause loop since we bypass it
    
    clean_dataset = load_dataset(CLEAN_DIR)
    rng = random.Random(42)
    
    # 1. Main Study Run (Primary)
    print("Running Primary batch study...")
    primary_batches = []
    for i in range(100): # scaled for API latency
        primary_batches.append(ru.run_batch(engine, clean_dataset, f"res-main-{i}", rng, batch_size=rng.randint(80, 150), error_count=rng.randint(8, 15)))
    primary_summary = ru.aggregate_batches("Primary Study", primary_batches)
    
    # 2. Lower & Higher Bound Runs
    print("Running Lower bound batches...")
    lower_batches = [ru.run_batch(engine, clean_dataset, f"res-low-{i}", rng, batch_size=5, error_count=2) for i in range(50)]
    lower_summary = ru.aggregate_batches("Lower Bound", lower_batches)
    
    print("Running Higher bound batches...")
    higher_batches = [ru.run_batch(engine, clean_dataset, f"res-high-{i}", rng, batch_size=150, error_count=15) for i in range(50)]
    higher_summary = ru.aggregate_batches("Higher Bound", higher_batches)
    
    # 3. Controlled Environment (Clean vs Error)
    print("Running Controlled clean run...")
    clean_batches = [ru.run_batch(engine, clean_dataset, f"res-clean-{i}", rng, batch_size=100, error_count=0) for i in range(50)]
    clean_summary = ru.aggregate_batches("Controlled Clean", clean_batches)
    
    print("Running Controlled error run...")
    error_batches = [ru.run_batch(engine, clean_dataset, f"res-err-{i}", rng, batch_size=100, error_count=10) for i in range(50)]
    error_summary = ru.aggregate_batches("Controlled Error", error_batches)
    
    # 4. Standard Procedure Runs
    print("Running SOP isolation tests...")
    sop_summary = {}
    for etype in ERROR_TYPES:
        batches = [ru.run_batch(engine, clean_dataset, f"sop-{etype}-{i}", rng, batch_size=50, specific_error_type=etype) for i in range(25)]
        sop_summary[etype] = ru.aggregate_sop_runs(etype, batches)
        
    # 5. Pairwise Combination Matrix
    print("Running Pairwise Permutation Combinations...")
    from pipeline_utils import inject_specific_error
    import time
    combinations_matrix = []
    
    # Use shorter names for the heatmap labels
    short_labels = [et.replace("_", " ").title().replace("Invalid ", "").replace("Unknown ", "") for et in ERROR_TYPES]
    
    for e1 in ERROR_TYPES:
        row_rates = []
        for e2 in ERROR_TYPES:
            # Shortened batches for pairwise to maintain execution speed
            resolved = 0
            detected = 0
            for i in range(15):
                clean_batch = ru.select_batch(clean_dataset, rng, size=80)
                dirty_1, err_1 = inject_specific_error(clean_batch, f"pair-{e1}-{e2}-{i}", e1, rng)
                if e1 == e2:
                    dirty_2, err_2 = dirty_1, err_1
                else:
                    dirty_2, err_2 = inject_specific_error(dirty_1, f"pair-{e1}-{e2}-{i}", e2, rng)
                
                detected_issues = engine._detect_issues(clean_batch, dirty_2, err_1 + err_2)
                healed_batch, log_lines, approvals = engine._resolve_issues(clean_batch, dirty_2, detected_issues)
                
                resolved += sum(1 for log in log_lines if log.get("status") == "resolved")
                detected += len(log_lines)
                
            rate = ru.pct(resolved, detected)
            row_rates.append(rate)
        combinations_matrix.append(row_rates)
        
    dataset_rows = [
        {"dataset": "Olist Subset", "rows": sum(b["batch_size"] for b in primary_batches), **primary_summary["metrics"]},
        {"dataset": "Low Volume", "rows": sum(b["batch_size"] for b in lower_batches), **lower_summary["metrics"]},
        {"dataset": "High Volume", "rows": sum(b["batch_size"] for b in higher_batches), **higher_summary["metrics"]},
    ]
    
    baselines = ru.load_baselines(BASE_DIR / "research_baselines.json")
    comparison_rows = baselines + [ru.project_comparison_row(primary_summary)]
    
    dataset_researchers_rows = ru.build_dataset_researchers_rows(dataset_rows, comparison_rows)
    
    # Fix for research_utils batch_size expectation
    lower_summary["batch_size"] = lower_batches[0]["batch_size"] if lower_batches else 5
    higher_summary["batch_size"] = higher_batches[0]["batch_size"] if higher_batches else 25

    payload = {
        "primary_summary": primary_summary,
        "controlled_clean": clean_summary,
        "controlled_error": error_summary,
        "dataset_rows": dataset_rows,
        "comparison_rows": comparison_rows,
        "dataset_researchers_rows": dataset_researchers_rows,
        "standard_procedure_rows": ru.build_standard_procedure_rows(sop_summary),
        "implementation_text": ru.build_implementation_text(primary_summary, lower_summary, higher_summary),
        "results_text": ru.build_results_text(primary_summary),
        "process_observation_text": ru.build_process_observation(lower_summary, higher_summary),
        "controlled_observation_text": ru.build_controlled_observation(clean_summary, error_summary),
        "dataset_observation_text": ru.build_dataset_observation(dataset_rows),
        "state_of_art_text": "Comparison of the proposed agentic pipeline against standard verification paradigms.",
    }
    
    out_dir = BASE_DIR / "web" / "research"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Write files
    print("Writing markdown and html reports...")
    ru.write_markdown_report(out_dir / "report.md", payload)
    ru.write_html_report(out_dir / "report.html", payload)
    
    # Write charts
    assets_dir = out_dir / "assets"
    
    methods = list(primary_summary["resolution_method_distribution"].items())
    methods.sort(key=lambda x: x[1], reverse=True)
    ru.write_pie_chart(assets_dir / "resolution_method_distribution.svg", "Resolution Methods Applied", methods)
    
    ru.write_heatmap(assets_dir / "error_combinations_heatmap.svg", "Pairwise Permutation Success Rates (%)", short_labels, short_labels, combinations_matrix)
    
    etype_labels = list(primary_summary["per_error_type"].keys())
    heatmap_matrix = [[primary_summary["per_error_type"][et]["heal_rate_pct"] for et in etype_labels]]
    ru.write_heatmap(assets_dir / "error_type_resolution_performance.svg", "Error Type Resolution Success", ["Success Rate (%)"], etype_labels, heatmap_matrix)
    
    # For dual bar chart (process bounds)
    cats = ["Lower Bound\\n(5 rows, 2 errs)", "Primary Study\\n(random)", "Higher Bound\\n(25 rows, 10 errs)"]
    simulated_times = [
        lower_summary["metrics"]["mttr_seconds"],
        primary_summary["metrics"]["mttr_seconds"],
        higher_summary["metrics"]["mttr_seconds"]
    ]
    actual_times = [
        lower_summary["metrics"]["avg_total_ms"],
        primary_summary["metrics"]["avg_total_ms"],
        higher_summary["metrics"]["avg_total_ms"]
    ]
    actual_errors = [
        max(0, lower_summary["metrics"]["p95_total_ms"] - actual_times[0]),
        max(0, primary_summary["metrics"]["p95_total_ms"] - actual_times[1]),
        max(0, higher_summary["metrics"]["p95_total_ms"] - actual_times[2]),
    ]
    ru.write_dual_bar_chart(assets_dir / "process_time_bounds.svg", "Processing Time Bounds", cats, simulated_times, actual_times, right_errors=actual_errors)
    
    # Dual bar chart for Error Types (SOP separation)
    categories_et = list(sop_summary.keys())
    sim_times_et = [sop_summary[c]["avg_resolution_sec"] for c in categories_et]
    act_times_et = [sop_summary[c]["avg_actual_ms"] for c in categories_et]
    act_errors_et = [max(0, sop_summary[c]["p95_actual_ms"] - act_times_et[idx]) for idx, c in enumerate(categories_et)]
    ru.write_dual_bar_chart(assets_dir / "error_types_timing.svg", "Processing Time by Error Type", categories_et, sim_times_et, act_times_et, right_errors=act_errors_et)

    # radar chart for SOA
    series = []
    metrics = ["precision", "recall", "f1_score", "accuracy", "auto_heal_rate"]
    colors = ["#999999", "#d9534f", "#2e86c1", "#8e44ad", "#2ecc71"]
    for idx, bench in enumerate(comparison_rows):
        series.append({
            "name": bench["system"],
            "values": [bench[m] for m in metrics],
            "color": colors[idx % len(colors)]
        })
    ru.write_radar_chart(assets_dir / "state_of_art_comparison.svg", "State-of-the-Art Benchmark Alignment", ["Precision", "Recall", "F1 Score", "Accuracy", "Auto-Heal %"], series)
        
if __name__ == "__main__":
    main()

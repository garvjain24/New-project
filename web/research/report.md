# Agentic Pipeline Research Report

## Implementation

The implementation uses the project’s live validation and repair engine as the measurement source. Each batch is sampled from the Olist-derived clean dataset, corrupted through controlled injection rules, and passed through the same detection, routing, healing, and escalation logic used by the simulator UI. In the benchmark configuration, 100 batches were processed for the main study, while lower-bound and higher-bound timing observations were taken under fixed batch windows of 5 and 150 rows respectively.

## Result

The primary experiment achieved precision 0.84, recall 0.73, F1-score 0.79, and accuracy 0.87. The auto-heal rate was 83.33% with an escalation rate of 5.90%. Average simulated MTTD was 5.44s and MTTR was 31.68s.

## Observation for Time for process lower and higher

Lower-bound runs completed in an average of 91.965 ms per batch, while higher-bound runs averaged 164.503 ms. The higher-load configuration added 72.538 ms on average, showing the expected increase in end-to-end processing time as both row volume and injected complexity rise.

## Observation for Time taken for controlled environment and errors

In the controlled environment without injected errors, average total batch time was 56.934 ms. With injected errors, average total batch time rose to 165.572 ms, a relative increase of 100.00%. This isolates the overhead introduced by validation failures, routing, and recovery logic.

## Different Dataset

Different dataset profiles show that Olist Subset processed 11536 rows with auto-heal 83.33% and avg total batch time 171.860 ms; Low Volume processed 250 rows with auto-heal 83.03% and avg total batch time 91.965 ms; High Volume processed 7500 rows with auto-heal 87.64% and avg total batch time 164.503 ms.

## Comparison with the state of the art

Comparison of the proposed agentic pipeline against standard verification paradigms.

## Dataset Researchers MetricsforComparison

- Dataset: Olist Subset | Researchers/System: This project | Metrics: precision, recall, f1_score, accuracy, auto_heal_rate, MTTD, MTTR | Note: Dynamic metrics generated from controlled experiments
- Dataset: Low Volume | Researchers/System: This project | Metrics: precision, recall, f1_score, accuracy, auto_heal_rate, MTTD, MTTR | Note: Dynamic metrics generated from controlled experiments
- Dataset: High Volume | Researchers/System: This project | Metrics: precision, recall, f1_score, accuracy, auto_heal_rate, MTTD, MTTR | Note: Dynamic metrics generated from controlled experiments
- Dataset: Benchmark Template | Researchers/System: Manual Review (Human-Only) | Metrics: precision, recall, f1_score, accuracy, auto_heal_rate, MTTD, MTTR | Note: Template baseline for manuscript drafting. Replace with a cited manual-review benchmark before publication.
- Dataset: Benchmark Template | Researchers/System: Rule-Based (Static Rules Only) | Metrics: precision, recall, f1_score, accuracy, auto_heal_rate, MTTD, MTTR | Note: Template baseline representing a fixed-rule validation pipeline. Replace with cited literature values.
- Dataset: Benchmark Template | Researchers/System: ML-Based (Supervised Classifier) | Metrics: precision, recall, f1_score, accuracy, auto_heal_rate, MTTD, MTTR | Note: Template baseline representing supervised ML detection and repair. Replace with cited literature values.
- Dataset: Benchmark Template | Researchers/System: Hybrid ML + Rules | Metrics: precision, recall, f1_score, accuracy, auto_heal_rate, MTTD, MTTR | Note: Template baseline representing hybrid ML plus deterministic repair logic. Replace with cited literature values.
- Dataset: Benchmark Template | Researchers/System: Proposed Agentic Pipeline | Metrics: precision, recall, f1_score, accuracy, auto_heal_rate, MTTD, MTTR | Note: This project

## Standard Procedures

- null_key: runs=25, heal_rate=0.00%, avg_resolution=182.62s, avg_actual=56.708ms
- duplicate_record: runs=25, heal_rate=93.35%, avg_resolution=6.85s, avg_actual=57.002ms
- invalid_payment_value: runs=25, heal_rate=94.90%, avg_resolution=11.02s, avg_actual=65.003ms
- orphan_record: runs=25, heal_rate=93.09%, avg_resolution=9.83s, avg_actual=58.905ms
- invalid_timestamp: runs=25, heal_rate=93.34%, avg_resolution=9.21s, avg_actual=62.370ms
- freight_outlier: runs=25, heal_rate=95.46%, avg_resolution=5.96s, avg_actual=56.827ms
- unknown_payment_type: runs=25, heal_rate=91.77%, avg_resolution=5.07s, avg_actual=58.041ms
- missing_column: runs=25, heal_rate=100.00%, avg_resolution=13.93s, avg_actual=54.753ms

## Chart Files

- `assets/resolution_method_distribution.svg`
- `assets/error_type_resolution_performance.svg`
- `assets/error_combinations_heatmap.svg`
- `assets/process_time_bounds.svg`
- `assets/error_types_timing.svg`
- `assets/state_of_art_comparison.svg`

## Publication Note

The state-of-the-art baseline rows are drafting templates. Replace their values and notes with properly cited literature before submitting a paper.
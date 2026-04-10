import json
import random
from server import SimulationEngine
from pipeline_utils import generate_dirty_dataset

engine = SimulationEngine()
clean = engine._load_clean()
batch = engine._select_batch(clean, random.Random())
dirty, errors = generate_dirty_dataset(batch, "cycle-test", 10)
detected = engine._detect_issues(clean, dirty, errors)

print("Starting resolution...")
healed, logs, approvals = engine._resolve_issues(clean, dirty, detected)

# Look at logs to see if LLM guided route or generated_local was hit.
for log in logs:
    if log["agent_route"] in ["llm_guided_local", "generate_local", "generated_local"]:
        print(f"[{log['error_type']}] Route: {log['agent_route']} | Resol: {log['resolution_action']}")

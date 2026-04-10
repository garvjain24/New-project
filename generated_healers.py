import json
from copy import deepcopy

def heal_duplicate_record(issue, healed, clean_lookup):
    dataset = issue['dataset']
    idx = issue['row_index']
    if dataset in healed and 0 <= idx < len(healed[dataset]):
        healed[dataset].pop(idx)

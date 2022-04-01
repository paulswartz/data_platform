#!/usr/bin/env python3
import os
import json
from datetime import timedelta
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow.csv as csv
from cubic_dmap import api, simplify
from cubic_dmap.state import State

apikey = os.environ['CUBIC_DMAP_API_KEY']
tmp_path = Path("tmp")
state_path = tmp_path / "state.json"

tmp_path.mkdir(parents=True, exist_ok=True)

if state_path.exists():
    with state_path.open(encoding="utf8") as f:
        state = State.from_dict(json.load(f))
else:
    state = State()

for endpoint in api.endpoints():
    print(endpoint)
    datasets = api.get(
        endpoint, apikey, last_updated=state.get_next_updated_time(endpoint))
    for dataset in datasets:
        print(dataset)
        table = dataset.fetch()
        csv.write_csv(table, tmp_path / f"{dataset.dataset_id}.csv")
        table = simplify.simplify_table(table)
        print(table)
        pq.write_table(table, tmp_path / f"{dataset.dataset_id}.parquet",
                       version="2.4", flavor="spark", compression="GZIP")
        state.update(dataset)
        print("")
    print("")
    assert [] == api.get(
        endpoint, apikey, last_updated=state.get_next_updated_time(endpoint))

print(state)

with state_path.open("w", encoding="utf8") as f:
    json.dump(state.to_dict(), f)

#!/usr/bin/env python3
import os
import json
from datetime import timedelta
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow.csv as csv
from cubic_dmap import api, simplify
from cubic_dmap.state import State


def write_csv(dataset, table):
    last_updated_iso_basic = dataset.last_updated.strftime(
        "%Y%m%dT%H%M%S.%f%z")
    csv_path = tmp_path / "archive" / dataset.id / \
        f"last_updated={last_updated_iso_basic}"
    csv_path.mkdir(parents=True, exist_ok=True)
    csv.write_csv(table, csv_path / f"{dataset.dataset_id}.csv")
    return None


def write_parquet(dataset, table):
    partition_cols = []
    for key in ["Year", "Month", "Day"]:
        if key in table.schema.names:
            partition_cols.append(key)
    pq.write_to_dataset(table, tmp_path / dataset.id,
                        partition_cols=partition_cols,
                        partition_filename_cb=lambda _: f"{dataset.dataset_id}.parquet",
                        version="2.4", flavor="spark", compression="GZIP")

    return None


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
        write_csv(dataset, table)
        table = simplify.simplify_table(table)
        print(table)
        write_parquet(dataset, table)
        state.update(dataset)
        print("")
    print("")
    assert [] == api.get(
        endpoint, apikey, last_updated=state.get_next_updated_time(endpoint))

print(state)

with state_path.open("w", encoding="utf8") as f:
    json.dump(state.to_dict(), f)

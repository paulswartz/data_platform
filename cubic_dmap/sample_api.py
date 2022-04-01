#!/usr/bin/env python3
import os
import json
import pyarrow as pa
from pyarrow import csv, fs
import pyarrow.dataset as ds
from cubic_dmap import api, simplify
from cubic_dmap.dataset import Dataset
from cubic_dmap.state import State

apikey = os.environ['CUBIC_DMAP_API_KEY']
(state_fs, state_path) = fs.FileSystem.from_uri(
    os.environ["STATE_FS"])
(archive_fs, archive_path) = fs.FileSystem.from_uri(
    os.environ["DMAP_ARCHIVE_FS"])
(springboard_fs, springboard_path) = fs.FileSystem.from_uri(
    os.environ["DMAP_SPRINGBOARD_FS"])


def write_csv(dataset: Dataset, table: pa.Table) -> None:
    last_updated_iso_basic = dataset.last_updated.strftime(
        "%Y%m%dT%H%M%S.%f%z")
    csv_path = f"{archive_path}/{dataset.id}/last_updated={last_updated_iso_basic}"
    archive_fs.create_dir(csv_path, recursive=True)
    with archive_fs.open_output_stream(f"{csv_path}/{dataset.dataset_id}.csv") as f:
        csv.write_csv(table, f)


def write_parquet(dataset: Dataset, table: pa.Table) -> None:
    partition_cols = []
    for key in ["Year", "Month", "Day"]:
        if key in table.schema.names:
            partition_cols.append(key)

    # map format arguments
    parquet_format = ds.ParquetFileFormat()
    write_options = parquet_format.make_write_options(
        version="2.4", compression="GZIP")

    if partition_cols:
        part_schema = table.select(partition_cols).schema
        partitioning = ds.partitioning(part_schema, flavor="hive")
    else:
        partitioning = None

    ds.write_dataset(
        table, f"{springboard_path}/{dataset.id}", filesystem=springboard_fs,
        basename_template=f"{dataset.dataset_id}-{'{i}'}.parquet",
        format=parquet_format, file_options=write_options, schema=table.schema,
        partitioning=partitioning, use_threads=True,
        existing_data_behavior="overwrite_or_ignore")


def load_state() -> State:
    try:
        with state_fs.open_input_stream(state_path) as f:
            return State.from_dict(json.load(f))
    except (FileNotFoundError, OSError):
        return State()


def fetch_endpoints(state: State, output=print) -> State:
    for endpoint in api.endpoints():
        output(endpoint)
        datasets = api.get(
            endpoint, apikey, last_updated=state.get_next_updated_time(endpoint))
        for dataset in datasets:
            output(dataset)
            table = dataset.fetch()
            write_csv(dataset, table)
            table = simplify.simplify_table(table)
            output(table)
            write_parquet(dataset, table)
            state.update(dataset)
            output("")
        output("")
        assert [] == api.get(
            endpoint, apikey, last_updated=state.get_next_updated_time(endpoint))

    return state


def write_state(state: State) -> None:
    with state_fs.open_output_stream(state_path) as f:
        str_output = json.dumps(state.to_dict())
        f.write(str_output.encode('utf8'))


if __name__ == "__main__":
    state = load_state()
    state = fetch_endpoints(state)
    write_state(state)
    print(state)

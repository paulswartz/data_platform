#!/usr/bin/env python3
import os
import json
import traceback
from typing import Any
import pyarrow as pa
from pyarrow import csv, fs
import pyarrow.dataset as ds
from cubic_dmap import api, simplify
from cubic_dmap.dataset import Dataset
from cubic_dmap.state import State

public_apikey = os.environ['CUBIC_DMAP_PUBLIC_API_KEY']
controlled_apikey = os.environ['CUBIC_DMAP_CONTROLLED_API_KEY']

(state_fs, state_path) = fs.FileSystem.from_uri(
    os.environ["STATE_FS"])
(error_fs, error_path) = fs.FileSystem.from_uri(
    os.environ["DMAP_ERROR_FS"])
(archive_fs, archive_path) = fs.FileSystem.from_uri(
    os.environ["DMAP_ARCHIVE_FS"])
(springboard_fs, springboard_path) = fs.FileSystem.from_uri(
    os.environ["DMAP_SPRINGBOARD_FS"])


def archive_csv(dataset: Dataset, table: pa.Table) -> None:
    last_updated_iso_basic = dataset.last_updated.strftime(
        "%Y%m%dT%H%M%S.%f%z")
    csv_path = f"{archive_path}/{dataset.id}/last_updated={last_updated_iso_basic}"
    archive_fs.create_dir(csv_path, recursive=True)
    with archive_fs.open_output_stream(f"{csv_path}/{dataset.dataset_id}.csv") as f:
        csv.write_csv(table, f)


def error_csv(dataset: Dataset, error: Any, table: pa.Table) -> None:
    last_updated_iso_basic = dataset.last_updated.strftime(
        "%Y%m%dT%H%M%S.%f%z")
    csv_path = f"{error_path}/{dataset.id}/last_updated={last_updated_iso_basic}"
    error_fs.create_dir(csv_path, recursive=True)
    with error_fs.open_output_stream(f"{csv_path}/{dataset.dataset_id}.csv") as f:
        csv.write_csv(table, f)

    with error_fs.open_output_stream(f"{csv_path}/error.txt") as f:
        f.write(str(error).encode('utf8'))


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
    for (endpoint_fn, apikey) in (
            (api.public_user_endpoints, public_apikey),
            (api.controlled_user_endpoints, controlled_apikey)
    ):
        for endpoint in endpoint_fn():
            output(endpoint)
            datasets = api.get(
                endpoint, apikey, last_updated=state.get_next_updated_time(endpoint))
            for dataset in datasets:
                fetch_and_update_dataset(dataset, state, output=output)
                output("")
            output("")
        output("")

    return state


def fetch_and_update_dataset(dataset: Dataset, state: State, output) -> None:
    output(dataset)
    state.update(dataset)
    with dataset.table() as table:
        try:
            try:
                simple_table = simplify.simplify_table(table)
            except Exception:   # pylint: disable=broad-except
                # don't fail when simplifying the table
                simple_table = table
            output(simple_table)
            write_parquet(dataset, simple_table)
            archive_csv(dataset, table)
        except Exception:  # pylint: disable=broad-except
            formatted_error = traceback.format_exc()
            output(formatted_error)
            error_csv(dataset, formatted_error, table)


def write_state(state: State) -> None:
    with state_fs.open_output_stream(state_path) as f:
        str_output = json.dumps(state.to_dict())
        f.write(str_output.encode('utf8'))


if __name__ == "__main__":
    state = load_state()
    state = fetch_endpoints(state)
    write_state(state)
    print(state)

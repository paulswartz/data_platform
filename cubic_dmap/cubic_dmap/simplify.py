from typing import Callable
from datetime import date, datetime
import pyarrow as pa


def simplify_table(table: pa.Table) -> pa.Table:
    """
    - convert Date columns to date32 (from string)
    - convert Day columns to DD string
    - convert Month columns to MM string (from string)
    - convert Year columns to YYYY string (from int64)
    - convert some column types to dictionary encoding (if it saves space):
      - Day Type
      - Time Period
      - Service
      - Fare Product Type
      - Route
      - Station
    """
    table = replace_column(table, "Year", simplify_year_column)
    table = replace_column(table, "Month", simplify_month_column)
    if "Date" in table.schema.names:
        table = update_date_columns(table)
    for column in {
        "Day Type",
        "Time Period",
        "Service",
        "Fare Product Type",
        "Route",
        "Station",
    }:
        table = replace_column(table, column, dictionarize_column)

    return table.unify_dictionaries()


def replace_column(
    table: pa.Table, field: str, replace_fun: Callable[[pa.Array], pa.Array]
) -> pa.Table:
    schema_names = table.schema.names
    if field not in schema_names:
        return table

    index = schema_names.index(field)
    column = table.column(index)
    new_column = replace_fun(column)
    return table.set_column(index, field, new_column)


DATE_EPOCH = date(1970, 1, 1)


def update_date_columns(table: pa.Table) -> pa.Table:
    if table.schema.field("Date").type == pa.date32():
        dates = table.column("Date").to_pylist()
    else:
        dates = [parse_date(d) for d in table.column("Date").to_pylist()]
        date_column = pa.array([(d - DATE_EPOCH).days for d in dates], pa.date32())
        table = replace_column(table, "Date", lambda _: date_column)

    year_column = pa.array([d.strftime("%Y") for d in dates])
    month_column = pa.array([d.strftime("%m") for d in dates])
    day_column = pa.array([d.strftime("%d") for d in dates])
    return (
        table.append_column("Year", year_column)
        .append_column("Month", month_column)
        .append_column("Day", day_column)
    )


def parse_date(d: str) -> date:
    if len(d) == 8:
        # agg_hourly_entry_exit_count
        return datetime.strptime(d, "%m-%d-%y").date()
    try:
        return datetime.strptime(d, "%m-%d-%Y").date()
    except ValueError:
        # agg_daily_fareprod_station
        return datetime.strptime(d, "%B %d, %Y").date()


def simplify_year_column(array: pa.Array) -> pa.Array:
    return array.cast(pa.string())


def simplify_month_column(array: pa.Array) -> pa.Array:
    return pa.array(
        [datetime.strptime(d, "%B").strftime("%m") for d in array.to_pylist()],
        pa.string(),
    )


def dictionarize_column(array: pa.Array) -> pa.Array:
    new_array = array.dictionary_encode()
    if new_array.nbytes < array.nbytes:
        return new_array

    return array

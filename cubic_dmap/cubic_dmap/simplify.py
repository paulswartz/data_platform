from typing import Iterable, Callable, Optional, Union
from datetime import date, datetime
from uuid import UUID
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
    table = replace_column_suffix(table, "_year", simplify_year_column)
    table = replace_column(table, "Month", simplify_month_column)
    table = replace_column_suffix(table, "_month_nbr", simplify_uint8_column)
    table = replace_column(table, "Date", simplify_date_column)
    table = replace_column_suffix(table, "_mm_dd_yy", simplify_date_column)
    table = replace_column(table, "Hour", simplify_uint8_column)
    table = replace_column(table, "id", simplify_uuid_column)
    table = replace_column_suffix(table, "_flag", simplify_boolean_column)
    table = replace_column_suffix(table, "_name", dictionarize_column)
    table = replace_column_suffix(table, "_desc", dictionarize_column)
    if "Date" in table.schema.names:
        table = set_date_partition_columns(table, "Date")
    elif "transit_day_mm_dd_yy" in table.schema.names:
        table = set_date_partition_columns(table, "transit_day_mm_dd_yy")
    elif "event_day_mm_dd_yy" in table.schema.names:
        table = set_date_partition_columns(table, "event_day_mm_dd_yy")
    for column in {
        "Day Type",
        "Time Period",
        "Service",
        "Fare Product Type",
        "Route",
        "Station",
        "device_id",
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


def replace_column_suffix(
    table: pa.Table, suffix: str, replace_fun: Callable[[pa.Array], pa.Array]
) -> pa.Table:
    for (index, field) in enumerate(table.schema.names):
        if not field.endswith(suffix):
            continue

        column = table.column(index)
        new_column = replace_fun(column)
        table = table.set_column(index, field, new_column)

    return table


DATE_EPOCH = date(1970, 1, 1)


def set_date_partition_columns(table: pa.Table, column_name: str) -> pa.Table:
    dates = table.column(column_name).to_pylist()
    year_column = pa.array([d.strftime("%Y") for d in dates])
    month_column = pa.array([d.strftime("%m") for d in dates])
    day_column = pa.array([d.strftime("%d") for d in dates])
    return (
        table.append_column("Year", year_column)
        .append_column("Month", month_column)
        .append_column("Day", day_column)
    )


def simplify_date_column(array: pa.Array) -> pa.Array:
    dates = (parse_date(d) for d in array.to_pylist())
    return date_array_from_dates(dates)


def date_array_from_dates(dates: Iterable[Optional[date]]) -> pa.Array:
    return pa.array(
        [None if d is None else (d - DATE_EPOCH).days for d in dates], pa.date32()
    )


DATE_FORMATS = ["%m-%d-%y", "%m-%d-%Y", "%Y-%m-%d", "%B %d, %Y"]


def parse_date(d: Optional[Union[str, date]]) -> Optional[date]:
    if d is None:
        return None
    if d == "":
        return None
    if isinstance(d, date):
        return d
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(d, fmt).date()
        except ValueError:
            continue

    raise ValueError(f"unable to convert {d!r} to date")


def simplify_year_column(array: pa.Array) -> pa.Array:
    return array.cast(pa.string())


def simplify_month_column(array: pa.Array) -> pa.Array:
    return pa.array(
        [datetime.strptime(d, "%B").strftime("%m") for d in array.to_pylist()],
        pa.string(),
    )


def simplify_uint8_column(array: pa.Array) -> pa.Array:
    return array.cast(pa.uint8())


def simplify_boolean_column(array: pa.Array) -> pa.Array:
    return array.cast(pa.bool_())


def simplify_uuid_column(array: pa.Array) -> pa.Array:
    try:
        uuids = [UUID(hex=h) for h in array.to_pylist()]
    except ValueError:
        # not UUIDs
        return array

    binary_uuids = [u.bytes for u in uuids]
    return pa.array(binary_uuids, pa.binary(16))


def dictionarize_column(array: pa.Array) -> pa.Array:
    new_array = array.dictionary_encode()
    if new_array.nbytes < array.nbytes:
        return new_array

    return array

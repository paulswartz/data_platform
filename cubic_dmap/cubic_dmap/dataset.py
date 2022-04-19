from datetime import date, datetime
import gzip
from urllib.parse import urlparse
from attrs import frozen, field
from cattr.preconf import json
import requests
import pyarrow as pa
from pyarrow import csv


CONVERTER = json.make_converter()
CONVERTER.register_structure_hook(date, lambda value, _type: date.fromisoformat(value))


@frozen
class Dataset:
    """
    A single dataset as returned from the DMAP API.
    """

    dataset_id: str
    id: str
    start_date: date
    end_date: date
    last_updated: datetime
    url: str

    @classmethod
    def from_dict(klass, raw: str) -> "Dataset":
        """
        Convert a dictionary into a Dataset.

        Useful for processing decoded JSON.
        """
        return CONVERTER.structure(raw, klass)

    def filename(self) -> str:
        """
        Return the base filename from the URL.
        """
        return urlparse(self.url).path[1:]

    def fetch(self) -> pa.Table:
        """
        Return the contents of this dataset as a PyArrow Table.
        """
        r = requests.get(self.url)
        csv_file = pa.BufferReader(r.content)

        if self.filename().endswith(".gz"):
            csv_file = pa.CompressedInputStream(csv_file, "gzip")

        parse_options = csv.ParseOptions(newlines_in_values=True)
        return csv.read_csv(csv_file, parse_options=parse_options)

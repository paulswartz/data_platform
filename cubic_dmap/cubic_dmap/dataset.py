from datetime import date, datetime
import gzip
from urllib.parse import urlparse
from tempfile import NamedTemporaryFile
from contextlib import contextmanager
from typing import Callable, Generator, IO, Optional, Tuple
from attrs import frozen, field
from cattr.preconf import json
import requests
import pyarrow as pa
from pyarrow import csv


CONVERTER = json.make_converter()
CONVERTER.register_structure_hook(date, lambda value, _type: date.fromisoformat(value))

BLOCK_SIZE = 1024 * 1024


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
        return urlparse(self.url).path.rsplit("/", maxsplit=1)[1]

    def parse_options(self) -> csv.ParseOptions:
        """
        ParseOptions for reading CSV files with PyArrow.
        """
        return csv.ParseOptions(newlines_in_values=True)

    @contextmanager
    def table(
        self, block_callback: Optional[Callable[[int], None]] = None, **kwargs
    ) -> Generator[
        Tuple[IO[bytes], Optional[pa.Table], Optional[Exception]], None, None
    ]:
        """
        Return the"""
        suffix = self.filename()

        r = requests.get(self.url, stream=True)

        if r.headers.get("content-encoding") == "gzip":
            suffix += ".gz"

        with NamedTemporaryFile(mode="wb+", suffix=suffix) as f:
            block_count = 0
            while True:
                data = r.raw.read(BLOCK_SIZE)
                if data == b"":
                    break
                f.write(data)
                if block_callback:
                    block_callback(block_count)
                block_count += 1

            if "parse_options" not in kwargs:
                kwargs["parse_options"] = self.parse_options()

            f.flush()
            try:
                table = csv.read_csv(f.name, **kwargs)
            except Exception as e:
                f.seek(0)
                yield (f, None, e)
            else:
                f.seek(0)
                yield (f, table, None)

    def fetch(self) -> pa.Table:
        """
        Return the contents of this dataset as a PyArrow Table.
        """
        r = requests.get(self.url)
        csv_file = pa.BufferReader(r.content)

        if self.filename().endswith(".gz"):
            csv_file = pa.CompressedInputStream(csv_file, "gzip")

        return csv.read_csv(csv_file, parse_options=self.parse_options())

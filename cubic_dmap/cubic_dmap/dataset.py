from datetime import date, datetime
from io import BytesIO
from attrs import frozen, field
import cattrs
import requests
import pyarrow as pa
from pyarrow import csv


CONVERTER = cattrs.Converter()

CONVERTER.register_structure_hook(date, lambda value, _type: date.fromisoformat(value))
CONVERTER.register_structure_hook(
    datetime, lambda value, _type: datetime.fromisoformat(value)
)


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

    def fetch(self) -> pa.Table:
        """
        Return the contents of this dataset as a PyArrow Table.
        """
        csv_file = BytesIO(requests.get(self.url).content)
        return csv.read_csv(csv_file)

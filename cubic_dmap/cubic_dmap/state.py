from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from attrs import define, field
from cattr.preconf import json
from cubic_dmap.dataset import Dataset

CONVERTER = json.make_converter()


@define
class State:
    """
    Current state of the sync job.

    Currently the only state we track is the last_updated date for each endpoint.
    """

    last_updated: Dict[str, datetime] = field(factory=dict)

    def to_dict(self) -> Any:
        """
        Converts the state to something suitable for conversion to JSON.
        """
        return CONVERTER.unstructure(self)

    @classmethod
    def from_dict(klass, source: Any) -> "State":
        """
        Converts decoded JSON into a State.
        """
        return CONVERTER.structure(source, klass)

    def get_next_updated_time(self, endpoint: str) -> Optional[datetime]:
        """
        Returns the next `last_updated` time to use for a given endpoint.

        If no last_updated time is present, returns None to fetch all datasets.

        Otherwise, increments the current `last_updated` time for that endpoint by a
        millisecond.
        """
        if endpoint in self.last_updated:
            return self.last_updated[endpoint] + timedelta(milliseconds=1)

        return None

    def update(self, dataset: Dataset) -> None:
        """
        Update the state with the last_updated value from the provided dataset.
        """
        if dataset.id in self.last_updated:
            new_last_updated = max(self.last_updated[dataset.id], dataset.last_updated)
        else:
            new_last_updated = dataset.last_updated

        self.last_updated[dataset.id] = new_last_updated

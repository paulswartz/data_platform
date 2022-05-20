from datetime import date, datetime, timedelta
import json
from cubic_dmap.dataset import Dataset
from cubic_dmap.state import State

endpoint = "endpoint"
dataset = Dataset(
    id=endpoint,
    dataset_id=f"{endpoint}_2022",
    start_date=date(2022, 1, 1),
    end_date=date(2022, 12, 31),
    last_updated=datetime.utcnow(),
    url="https://mbta.com/",
)


class TestUpdatedTime:
    def test_initial_state(self):
        state = State()
        assert state.get_next_updated_time(endpoint) is None

    def test_after_update(self):
        state = State()
        state.update(dataset)
        diff = state.get_next_updated_time(endpoint) - dataset.last_updated
        assert diff == timedelta(milliseconds=1)


class TestJsonSerialization:
    def test_round_trip_empty(self):
        assert self.round_trip(State()) == State()

    def test_round_trip_after_update(self):
        state = State()
        state.update(dataset)

        assert self.round_trip(state) == state

    @staticmethod
    def round_trip(state):
        json_encoded_str = json.dumps(state.to_dict())
        return State.from_dict(json.loads(json_encoded_str))

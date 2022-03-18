from datetime import date, datetime
from cubic_dmap import api


class TestProcessGetParams:
    def test_converts_dates_times(self):
        params = {
            "ignored": "value",
            "start_date": date(2022, 1, 1),
            "end_date": date(2022, 2, 2),
            "last_updated": datetime(2022, 3, 4, 5, 6, 7, 100000),
        }

        expected = {
            "ignored": "value",
            "start_date": "2022-01-01",
            "end_date": "2022-02-02",
            "last_updated": "2022-03-04T05:06:07.100000",
        }

        actual = api.process_get_params(params)

        assert expected == actual

    def test_converts_numbers(self):
        params = {"int": 1}
        expected = {"int": "1"}
        actual = api.process_get_params(params)

        assert expected == actual

    def test_drops_None(self):
        params = {"none": None}
        expected = {}
        actual = api.process_get_params(params)

        assert expected == actual

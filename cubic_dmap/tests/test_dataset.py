from datetime import date, datetime
import pyarrow as pa
from cubic_dmap import dataset


class TestDataset:
    def test_from_json(self):
        raw = {
            "dataset_id": "agg_boardings_fareprod_mode_month_2022",
            "end_date": "2022-12-31",
            "id": "agg_boardings_fareprod_mode_month",
            "last_updated": "2022-03-14T13:13:34.797248",
            "start_date": "2022-01-01",
            "url": "https://mbta.com",
        }

        expected = dataset.Dataset(
            dataset_id="agg_boardings_fareprod_mode_month_2022",
            id="agg_boardings_fareprod_mode_month",
            start_date=date(2022, 1, 1),
            end_date=date(2022, 12, 31),
            last_updated=datetime(2022, 3, 14, 13, 13, 34, 797248),
            url="https://mbta.com",
        )

        actual = dataset.Dataset.from_dict(raw)

        assert actual == expected

    def test_filename(self):
        ds = dataset.Dataset(
            dataset_id="archived_feeds",
            id="archived_Feeds",
            start_date=date(2022, 1, 1),
            end_date=date(2022, 12, 31),
            last_updated=datetime.utcnow(),
            url="https://cdn.mbta.com/archive/archived_feeds.txt",
        )

        assert ds.filename() == "archived_feeds.txt"

    def test_fetch(self):
        ds = dataset.Dataset(
            dataset_id="archived_feeds",
            id="archived_Feeds",
            start_date=date(2022, 1, 1),
            end_date=date(2022, 12, 31),
            last_updated=datetime.utcnow(),
            url="https://cdn.mbta.com/archive/archived_feeds.txt",
        )

        table = ds.fetch()
        assert isinstance(table, pa.Table)
        assert table.column_names == [
            "feed_start_date",
            "feed_end_date",
            "feed_version",
            "archive_url",
            "archive_note",
        ]

    def test_table(self):
        ds = dataset.Dataset(
            dataset_id="archived_feeds",
            id="archived_Feeds",
            start_date=date(2022, 1, 1),
            end_date=date(2022, 12, 31),
            last_updated=datetime.utcnow(),
            url="https://cdn.mbta.com/archive/archived_feeds.txt",
        )

        with ds.table() as table:
            assert isinstance(table, pa.Table)
            assert table.column_names == [
                "feed_start_date",
                "feed_end_date",
                "feed_version",
                "archive_url",
                "archive_note",
            ]

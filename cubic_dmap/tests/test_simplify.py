import pyarrow as pa
from datetime import date
from cubic_dmap.simplify import simplify_table


class TestSimplifyTable:
    def test_date_column(self):
        table = pa.table(
            {"Date": ["03-22-2022", "03-22-22", "March 22, 2022"], "Value": [1, 2, 3]}
        )

        epoch_date = (date(2022, 3, 22) - date(1970, 1, 1)).days

        expected = pa.table(
            {
                "Date": pa.array(
                    [epoch_date, epoch_date, epoch_date], type=pa.date32()
                ),
                "Value": pa.array([1, 2, 3]),
                "Year": pa.array(["2022", "2022", "2022"]),
                "Month": pa.array(["03", "03", "03"]),
                "Day": pa.array(["22", "22", "22"]),
            }
        )

        actual = simplify_table(table)

        assert expected == actual

    def test_date_column_already_date(self):
        table = pa.table({"Date": pa.array([0], pa.date32())})

        expected = pa.table(
            {
                "Date": pa.array([date(1970, 1, 1)], pa.date32()),
                "Year": pa.array(["1970"]),
                "Month": pa.array(["01"]),
                "Day": pa.array(["01"]),
            }
        )

        actual = simplify_table(table)

        assert expected == actual

    def test_year_month_hour_columns(self):
        table = pa.table(
            {
                "Year": pa.array([2022, 65535], type=pa.int64()),
                "Month": ["January", "December"],
                "Hour": pa.array([0, 23], type=pa.int64()),
            }
        )
        expected = pa.table(
            {
                "Year": pa.array(["2022", "65535"]),
                "Month": pa.array(["01", "12"]),
                "Hour": pa.array([0, 23], type=pa.uint8()),
            }
        )

        actual = simplify_table(table)

        assert expected == actual

    def test_dictionarize_columns(self):
        table = pa.table(
            {
                "Day Type": ["Weekday", "Weekday"],
                "Time Period": ["Midday", "Midday"],
                "Service": ["Bus", "Bus"],
                "Fare Product Type": ["Stored Value", "Stored Value"],
                "Station": ["Forest Hills", "Forest Hills"],
            }
        )

        actual = simplify_table(table)

        assert table.to_pylist() == actual.to_pylist()
        assert table.nbytes > actual.nbytes

defmodule CubicDmapTest do
  use ExUnit.Case, async: true
  doctest CubicDmap

  alias Explorer.DataFrame

  describe "csv_dataframe/1" do
    test "fetches a dataframe from a CSV url" do
      url = "https://cdn.mbta.com/archive/archived_feeds.txt"

      assert {:ok, df} = CubicDmap.csv_dataframe(url)

      assert DataFrame.names(df) == [
               "feed_start_date",
               "feed_end_date",
               "feed_version",
               "archive_url",
               "archive_note"
             ]

      assert DataFrame.dtypes(df) == [:integer, :integer, :string, :string, :string]
    end
  end
end

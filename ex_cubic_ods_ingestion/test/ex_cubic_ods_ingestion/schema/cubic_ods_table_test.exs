defmodule ExCubicOdsIngestion.Schema.CubicOdsTableTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsTable

  setup do
    # Explicitly get a connection before each test
    # @todo check out https://github.com/mbta/draft/blob/main/test/support/data_case.ex
    :ok = Sandbox.checkout(Repo)

    table = Repo.insert!(MockExAws.Data.table())

    {:ok, %{table: table}}
  end

  describe "get/1" do
    test "adding and getting table rec", %{table: inserted_table_rec} do
      assert inserted_table_rec == CubicOdsTable.get!(inserted_table_rec.id)
    end
  end

  describe "filter_to_existing_prefixes/1" do
    test "limits the provided prefixes to those with an existing table", %{table: table} do
      prefixes = [
        "vendor/SAMPLE/",
        "vendor/SAMPLE__ct/",
        "vendor/SAMPLE_TABLE_WRONG/",
        "other"
      ]

      expected = [
        {"vendor/SAMPLE/", table},
        {"vendor/SAMPLE__ct/", table}
      ]

      actual = CubicOdsTable.filter_to_existing_prefixes(prefixes)

      assert expected == actual
    end
  end

  describe "update/2" do
    test "updating the snapshot", %{table: inserted_table_rec} do
      dt = DateTime.now!("Etc/UTC")
      dt_without_msec = DateTime.truncate(dt, :second)
      updated_table_rec = CubicOdsTable.update(inserted_table_rec, snapshot: dt_without_msec)

      assert dt_without_msec == updated_table_rec.snapshot
    end
  end
end

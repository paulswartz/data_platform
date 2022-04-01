defmodule ExCubicOdsIngestion.Schema.CubicOdsTableTest do
  use ExCubicOdsIngestion.DataCase, async: true

  alias ExCubicOdsIngestion.Schema.CubicOdsTable

  setup do
    table = Repo.insert!(MockExAws.Data.table())

    {:ok, %{table: table}}
  end

  describe "get/1" do
    test "adding and getting table rec", %{table: inserted_table_rec} do
      assert inserted_table_rec == CubicOdsTable.get!(inserted_table_rec.id)
    end
  end

  describe "filter_to_existing_prefixes/1" do
    test "providing empty prefixes list" do
      assert [] == CubicOdsTable.filter_to_existing_prefixes([])
    end

    test "limits the provided prefixes to those with an existing table", %{table: table} do
      # note: purposely leaving out incoming bucket prefix config
      prefixes = [
        "cubic_ods_qlik/SAMPLE/",
        "cubic_ods_qlik/SAMPLE__ct/",
        "cubic_ods_qlik/SAMPLE_TABLE_WRONG/",
        "other"
      ]

      expected = [
        {"cubic_ods_qlik/SAMPLE/", table},
        {"cubic_ods_qlik/SAMPLE__ct/", table}
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

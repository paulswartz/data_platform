defmodule ExCubicIngestion.Workers.ErrorTest do
  use ExCubicIngestion.DataCase, async: true
  use Oban.Testing, repo: ExCubicIngestion.Repo

  alias ExCubicIngestion.Schema.CubicLoad
  alias ExCubicIngestion.TestFixtures
  alias ExCubicIngestion.Workers.Error

  require MockExAws

  setup do
    TestFixtures.setup_tables_loads()
  end

  describe "perform/1" do
    test "run job without error", %{
      dmap_load: dmap_load
    } do
      assert :ok ==
               perform_job(Error, %{
                 load_rec_id: dmap_load.id,
                 lib_ex_aws: "MockExAws"
               })

      assert "errored" == CubicLoad.get!(dmap_load.id).status
    end
  end

  describe "construct_destination_key/1" do
    test "getting destination key for generic load", %{
      dmap_load: dmap_load
    } do
      assert dmap_load.s3_key == Error.construct_destination_key(dmap_load)
    end

    test "getting destination key for ODS load", %{
      ods_load: ods_load
    } do
      assert "cubic/ods_qlik/SAMPLE/timestamp=20220101T204950Z/LOAD1.csv" ==
               Error.construct_destination_key(ods_load)
    end
  end
end

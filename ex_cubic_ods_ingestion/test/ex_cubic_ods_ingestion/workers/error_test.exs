defmodule ExCubicOdsIngestion.Workers.ErrorTest do
  use ExUnit.Case
  use Oban.Testing, repo: ExCubicOdsIngestion.Repo

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Workers.Error

  require MockExAws

  setup do
    # Explicitly get a connection before each test
    # @todo check out https://github.com/mbta/draft/blob/main/test/support/data_case.ex
    :ok = Sandbox.checkout(Repo)
  end

  describe "perform/1" do
    test "run job without error" do
      # insert a new table
      new_table_rec = Repo.insert!(MockExAws.Data.table())

      # insert load records
      {:ok, new_load_recs} =
        CubicOdsLoad.insert_new_from_objects_with_table(
          MockExAws.Data.load_objects(),
          new_table_rec
        )

      first_load_rec = List.first(new_load_recs)

      assert :ok ==
               perform_job(Error, %{
                 load_rec_id: first_load_rec.id,
                 lib_ex_aws: "MockExAws"
               })

      assert "errored" == CubicOdsLoad.get!(first_load_rec.id).status
    end
  end
end
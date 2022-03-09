defmodule ExCubicOdsIngestion.Schema.CubicOdsLoadTest do
  use ExUnit.Case

  import Ecto.Changeset

  alias Ecto.Adapters.SQL.Sandbox
  alias ExCubicOdsIngestion.Repo
  alias ExCubicOdsIngestion.Schema.CubicOdsLoad
  alias ExCubicOdsIngestion.Schema.CubicOdsTable

  setup do
    # Explicitly get a connection before each test
    # @todo check out https://github.com/mbta/draft/blob/main/test/support/data_case.ex
    :ok = Sandbox.checkout(Repo)

    table = Repo.insert!(MockExAws.Data.table())
    {:ok, %{table: table}}
  end

  describe "insert_new_from_objects_with_table/1" do
    test "providing a non-empty list of objects", %{table: table} do
      {:ok, new_load_recs} =
        CubicOdsLoad.insert_new_from_objects_with_table(MockExAws.Data.load_objects(), table)

      assert [
               %{
                 status: "ready",
                 s3_key: "vendor/SAMPLE/LOAD1.csv",
                 s3_modified: ~U[2022-02-08 20:49:50Z],
                 s3_size: 197
               },
               %{
                 status: "ready",
                 s3_key: "vendor/SAMPLE/LOAD2.csv",
                 s3_modified: ~U[2022-02-08 20:50:50Z],
                 s3_size: 123
               }
             ] ==
               Enum.map(new_load_recs, fn new_load_rec ->
                 %{
                   status: new_load_rec.status,
                   s3_key: new_load_rec.s3_key,
                   s3_modified: new_load_rec.s3_modified,
                   s3_size: new_load_rec.s3_size
                 }
               end)
    end

    test "providing an empty list of objects", %{table: table} do
      assert {:ok, []} == CubicOdsLoad.insert_new_from_objects_with_table([], table)
    end
  end

  describe "get_by_objects/1" do
    test "getting records just added by providing the list we added from", %{table: table} do
      load_objects = MockExAws.Data.load_objects()
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects_with_table(load_objects, table)

      assert new_load_recs ==
               CubicOdsLoad.get_by_objects(load_objects)
    end

    test "getting no records by providing a list with a load object not in db", %{table: table} do
      {:ok, _new_load_recs} =
        CubicOdsLoad.insert_new_from_objects_with_table(MockExAws.Data.load_objects(), table)

      assert [] ==
               CubicOdsLoad.get_by_objects([
                 %{
                   e_tag: "\"ghi789\"",
                   key: "not/in/db.csv",
                   last_modified: "2022-02-08T21:49:50.000Z",
                   owner: nil,
                   size: "197",
                   storage_class: "STANDARD"
                 }
               ])
    end

    test "getting no records by providing an empty list" do
      assert [] == CubicOdsLoad.get_by_objects([])
    end

    # @todo test for improper load object map
  end

  describe "not_added/2" do
    test "object NOT found in database records" do
      load_object = List.first(MockExAws.Data.load_objects())

      load_recs = [
        %CubicOdsLoad{
          s3_key: "key/not/found.csv",
          s3_modified: ~U[2022-02-08 20:49:50Z]
        }
      ]

      assert CubicOdsLoad.not_added(load_object, load_recs)
    end

    test "object found in database records" do
      load_object = List.first(MockExAws.Data.load_objects())

      load_recs = [
        %CubicOdsLoad{
          s3_key: "vendor/SAMPLE/LOAD1.csv",
          s3_modified: ~U[2022-02-08 20:49:50Z]
        }
      ]

      refute CubicOdsLoad.not_added(load_object, load_recs)
    end
  end

  describe "get_status_ready/0" do
    test "getting load records with the status 'ready'", %{table: table} do
      # insert records as ready
      {:ok, new_load_recs} =
        CubicOdsLoad.insert_new_from_objects_with_table(MockExAws.Data.load_objects(), table)

      # set the first record to 'archived'
      {:ok, _archived_load_rec} =
        Repo.transaction(fn ->
          Repo.update!(change(List.first(new_load_recs), status: "archived"))
        end)

      ready_load_recs = CubicOdsLoad.get_status_ready()
      # filter down to the ones we just inserted
      filtered_ready_load_recs = Enum.filter(ready_load_recs, &Enum.member?(new_load_recs, &1))

      # assert that the last record inserted comes back
      assert [List.last(new_load_recs)] == filtered_ready_load_recs
    end
  end

  describe "update/2" do
    test "setting an 'archived' status", %{table: table} do
      # insert records as ready
      {:ok, new_load_recs} =
        CubicOdsLoad.insert_new_from_objects_with_table(MockExAws.Data.load_objects(), table)

      # use the first record
      first_load_rec = List.first(new_load_recs)

      # update it to 'archived' status
      updated_load_rec = CubicOdsLoad.update(first_load_rec, status: "archived")

      assert Repo.get!(CubicOdsLoad, first_load_rec.id) == updated_load_rec
    end
  end

  describe "get_many_with_table/1" do
    test "getting no records by passing empty list" do
      assert [] == CubicOdsLoad.get_many_with_table([])
    end

    test "getting no records by passing just inserted load records" do
      # insert records as ready
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())
      new_load_rec_ids = Enum.map(new_load_recs, fn new_load_rec -> new_load_rec.id end)

      assert [] == CubicOdsLoad.get_many_with_table(new_load_rec_ids)
    end

    test "getting records by passing load records with tables attached" do
      # insert records as ready
      {:ok, new_load_recs} = CubicOdsLoad.insert_new_from_objects(MockExAws.Data.load_objects())

      # use the first record
      first_load_rec = List.first(new_load_recs)

      # insert new table
      new_table_rec = %CubicOdsTable{
        name: "vendor__sample",
        s3_prefix: "vendor/SAMPLE/",
        snapshot_s3_key: "vendor/SAMPLE/LOAD1.csv"
      }

      {:ok, inserted_table_rec} =
        Repo.transaction(fn ->
          Repo.insert!(new_table_rec)
        end)

      # update table_id for first load rec
      CubicOdsLoad.update(first_load_rec, %{table_id: inserted_table_rec.id})

      assert [{first_load_rec.id, inserted_table_rec.id}] ==
               Enum.map(
                 CubicOdsLoad.get_many_with_table([first_load_rec.id]),
                 fn {load_rec, table_rec} ->
                   {load_rec.id, table_rec.id}
                 end
               )
    end
  end
end

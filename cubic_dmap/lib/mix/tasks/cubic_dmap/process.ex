defmodule Mix.Tasks.CubicDmap.Process do
  @moduledoc """
  Process a given set of DMAP data.

  Currently, only prints the latest monthly aggregations.

  ## Example

  $ mix cubic_dmap.process monthly
  """
  def run(args)

  def run([aggregation]) do
    # Ensure the app is configured
    Mix.Task.run("app.config")
    Application.ensure_all_started(:cubic_dmap)

    all_aggregations = aggregations()
    aggregation = String.to_atom(aggregation)

    if datasets = all_aggregations[aggregation] do
      run_aggregations(aggregation, datasets)
    else
      IO.puts(["Unable to find aggregation '", aggregation, "', did you mean:\n"])

      for {agg_key, _} <- all_aggregations do
        IO.puts(['* ', Atom.to_string(agg_key)])
      end

      System.halt(1)
    end
  end

  defp run_aggregations(aggregation, datasets) do
    for ds <- datasets do
      case CubicDmap.dataset_urls(ds, dataset_opts(aggregation)) do
        {:ok, results} ->
          for result <- results do
            IO.puts(inspect(result))
            url = result["url"]

            case CubicDmap.csv_dataframe(url) do
              {:ok, df} ->
                IO.puts(inspect(df))

              {:error, r} ->
                IO.puts("! unable fetch dataframe #{inspect(url)}: #{inspect(r)}")
            end
          end

        {:error, r} ->
          IO.puts("! unable to fetch dataset #{inspect(ds)}: #{inspect(r)}")
      end
    end
  end

  defp aggregations do
    Application.fetch_env!(:cubic_dmap, :aggregations)
  end

  defp dataset_opts(:monthly) do
    now = Date.utc_today()
    first_of_month = %{now | day: 1}
    last_of_previous_month = Date.add(first_of_month, -1)
    first_of_previous_month = %{last_of_previous_month | day: 1}

    [
      start_date: Date.to_iso8601(first_of_previous_month),
      end_date: Date.to_iso8601(last_of_previous_month)
    ]
  end
end

defmodule CubicDmap do
  @moduledoc """
  Documentation for `CubicDmap`.
  """

  @doc """
  """
  def dataset_urls(agg, opts \\ []) do
    with {:ok, body} <- get(api_url(agg, opts)),
         {:ok, %{"success" => true, "results" => results}} <- Jason.decode(body) do
      {:ok, results}
    end
  end

  def csv_dataframe(url) do
    filename =
      Path.join([System.tmp_dir(), "#{System.monotonic_time()}-#{System.unique_integer()}.csv"])

    case get(url) do
      {:ok, body} ->
        File.write!(filename, body, [:write, :exclusive])
        df = Explorer.DataFrame.read_csv(filename)
        _ = File.rm(filename)

        df

      other ->
        other
    end
  end

  defp get(url) do
    case Mojito.get(url) do
      {:ok, %{status_code: 200, body: body}} ->
        {:ok, body}

      {:ok, response} ->
        {:error, response}

      {:error, error} ->
        {:error, error}
    end
  end

  defp api_url(agg, opts) do
    base_url = Application.get_env(:cubic_dmap, :base_url)

    query = [apikey: Application.get_env(:cubic_dmap, :api_key)] ++ opts

    base_url
    |> URI.new!()
    |> URI.merge(Atom.to_string(agg))
    |> Map.put(:query, URI.encode_query(query))
    |> URI.to_string()
  end
end

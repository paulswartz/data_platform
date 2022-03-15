defmodule CubicDmap.MixProject do
  use Mix.Project

  def project do
    [
      app: :cubic_dmap,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mojito, "~> 0.7"},
      {:jason, "~> 1.3"},
      {:explorer, "~> 0.1.0-dev", github: "elixir-nx/explorer", branch: "main"}
    ]
  end
end

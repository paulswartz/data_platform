defmodule CubicDmap.MixProject do
  use Mix.Project

  def project do
    [
      app: :cubic_dmap,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [
        tool: LcovEx
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :mix]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mojito, "~> 0.7"},
      {:jason, "~> 1.3"},
      {:explorer, "~> 0.1.0-dev", github: "elixir-nx/explorer", branch: "main"},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.1", only: :dev, runtime: false},
      {:lcov_ex, "~> 0.2", only: [:dev, :test], runtime: false}
    ]
  end
end

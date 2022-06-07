import Config

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

config :ex_cubic_ingestion,
  ecto_repos: [ExCubicIngestion.Repo],
  run_migrations_at_startup?: false,
  start_app?: true

config :ex_cubic_ingestion, Oban,
  repo: ExCubicIngestion.Repo,
  plugins: [],
  queues: [
    archive: 5,
    error: 5,
    fetch_dmap: 1,
    ingest: 5
  ]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"
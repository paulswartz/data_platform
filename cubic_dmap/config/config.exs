import Config

config :cubic_dmap,
  base_url: "https://mbta-qa.api.cubicnextcloud.com/datasetpublicusersapi/aggregations/",
  api_key: :undefined,
  aggregations: %{
    monthly: [
      :agg_average_boardings_by_day_type_month,
      :agg_boardings_fareprod_mode_month,
      :agg_total_boardings_month_mode
    ]
  }

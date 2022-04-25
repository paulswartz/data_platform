"""
Wrapper around the two DMAP APIs: one for controlled users and one for public users.
"""
from datetime import date, datetime
from typing import Any, List, Mapping
import requests
from cubic_dmap import dataset


BASE_URLS = {
    "qa": "https://mbta-qa.api.cubicnextcloud.com/controlledresearchusersapi/",
    "eil": "https://mbta-eil.api.cubicnextcloud.com/controlledresearchusersapi/",
    "qa_public": "https://mbta-qa.api.cubicnextcloud.com/datasetpublicusersapi/",
    "eil_public": "https://mbta-eil.api.cubicnextcloud.com/datasetpublicusersapi/",
}

ENDPOINTS = {
    "agg_average_boardings_by_day_type_month": "aggregations/agg_average_boardings_by_day_type_month",
    "agg_boardings_fareprod_mode_month": "aggregations/agg_boardings_fareprod_mode_month",
    "agg_total_boardings_month_mode": "aggregations/agg_total_boardings_month_mode",
    "agg_hourly_entry_exit_count": "aggregations/agg_hourly_entry_exit_count",
    "agg_daily_fareprod_station": "aggregations/agg_daily_fareprod_station",
    "agg_daily_transfers_station": "aggregations/agg_daily_transfers_station",
    "agg_daily_transfers_route": "aggregations/agg_daily_transfers_route",
    "agg_daily_fareprod_route": "aggregations/agg_daily_fareprod_route",
    "use_transaction_longitudinal": "transactional/use_transaction_longitudinal",
    "use_transaction_location": "transactional/use_transaction_location",
    "sale_transaction": "transactional/sale_transaction",
    "device_event": "transactional/device_event",
    "citation": "transactional/citation",
}


def endpoints() -> List[str]:
    """
    Return the valid endpoints for APIs.
    """
    return list(ENDPOINTS.keys())


def process_get_params(params: Mapping[str, Any]) -> Mapping[str, str]:
    """
    Convert keyword parameters into string parameters for the DMAP API.

    - date is converted to ISO date string
    - datetime is converted to ISO date/time string
    """
    result = {}

    for key, value in params.items():
        if value is None:
            continue
        elif isinstance(value, (date, datetime)):
            result[key] = value.isoformat()
        else:
            result[key] = str(value)

    return result


def get(
    endpoint: str, apikey: str, environment: str = "qa", **kwargs
) -> List[dataset.Dataset]:
    """
    Gets the datasets for a given DMAP endpoint.

    # Example
    >>> get("agg_total_boardings_month_mode", "<apikey>")
    [dataset.Dataset(...)]
    """
    params = {"apikey": apikey, **process_get_params(kwargs)}

    r = requests.get(BASE_URLS[environment] + ENDPOINTS[endpoint], params=params)
    json = r.json()

    if not json["success"]:
        raise RuntimeError(f"error fetching {r.url}: {json}")

    return [dataset.Dataset.from_dict(j) for j in json["results"]]

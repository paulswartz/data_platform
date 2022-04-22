"""
Wrapper around the two DMAP APIs: one for controlled users and one for public users.
"""
from datetime import date, datetime
from typing import Any, List, Mapping
import requests
from cubic_dmap import dataset


BASE_URLS = {
    "qa": "https://mbta-qa.api.cubicnextcloud.com/",
    "eil": "https://mbta-eil.api.cubicnextcloud.com/",
}

PUBLIC_ENDPOINTS = {
    "agg_average_boardings_by_day_type_month": "datasetpublicusersapi/aggregations/agg_average_boardings_by_day_type_month",
    "agg_boardings_fareprod_mode_month": "datasetpublicusersapi/aggregations/agg_boardings_fareprod_mode_month",
    "agg_total_boardings_month_mode": "datasetpublicusersapi/aggregations/agg_total_boardings_month_mode",
    "agg_hourly_entry_exit_count": "datasetpublicusersapi/aggregations/agg_hourly_entry_exit_count",
    "agg_daily_fareprod_station": "datasetpublicusersapi/aggregations/agg_daily_fareprod_station",
    "agg_daily_transfers_station": "datasetpublicusersapi/aggregations/agg_daily_transfers_station",
    "agg_daily_transfers_route": "datasetpublicusersapi/aggregations/agg_daily_transfers_route",
    "agg_daily_fareprod_route": "datasetpublicusersapi/aggregations/agg_daily_fareprod_route",
}

CONTROLLED_ENDPOINTS = {
    "use_transaction_longitudinal": "controlledresearchusersapi/transactional/use_transaction_longitudinal",
    "use_transaction_location": "controlledresearchusersapi/transactional/use_transaction_location",
    "sale_transaction": "controlledresearchusersapi/transactional/sale_transaction",
    "device_event": "controlledresearchusersapi/transactional/device_event",
    "citation": "controlledresearchusersapi/transactional/citation",
}

ENDPOINTS = {**PUBLIC_ENDPOINTS, **CONTROLLED_ENDPOINTS}


def public_user_endpoints() -> List[str]:
    """
    Return the valid endpoints for DMAP Public User APIs.
    """
    return list(PUBLIC_ENDPOINTS.keys())


def controlled_user_endpoints() -> List[str]:
    """
    Return the valid endpoints for DMAP Controlled User APIs.
    """
    return list(CONTROLLED_ENDPOINTS.keys())


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

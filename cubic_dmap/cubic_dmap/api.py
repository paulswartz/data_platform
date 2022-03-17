"""
Wrapper around the two DMAP APIs: one for controlled users and one for public users.
"""
from typing import List
import requests
from cubic_dmap import dataset


BASE_URLS = {
    "qa": "https://mbta-qa.api.cubicnextcloud.com/",
}

ENDPOINTS = {
    "agg_average_boardings_by_day_type_month": "datasetpublicusersapi/aggregations/agg_average_boardings_by_day_type_month",
    "agg_boardings_fareprod_mode_month": "datasetpublicusersapi/aggregations/agg_boardings_fareprod_mode_month",
    "agg_total_boardings_month_mode": "datasetpublicusersapi/aggregations/agg_total_boardings_month_mode",
}


def get(
    endpoint: str, apikey: str, environment: str = "qa", **kwargs
) -> List[dataset.Dataset]:
    """
    Gets the datasets for a given DMAP endpoint.

    # Example
    >>> get("agg_total_boardings_month_mode", "<apikey>")
    [dataset.Dataset(...)]
    """
    params = {"apikey": apikey, **kwargs}

    json = requests.get(
        BASE_URLS[environment] + ENDPOINTS[endpoint], params=params
    ).json()

    if not json["success"]:
        raise RuntimeError(str(json))

    return [dataset.Dataset.from_dict(j) for j in json["results"]]

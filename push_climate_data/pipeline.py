import json
import logging
import os
from typing import List

import pandas as pd
import requests
from openhexa.sdk import current_run, parameter, pipeline, workspace
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class DHIS2ImportError(Exception):
    pass


@pipeline("push-climate-data", name="Push Climate Data")
@parameter(
    "import_strategy",
    name="Import strategy",
    help="Choose the DHIS2 import strategy",
    choices=["CREATE", "UPDATE", "CREATE_AND_UPDATE", "DELETE"],
    type=str,
    required=True,
    default="CREATE",
)
@parameter(
    "limit_variable",
    name="Climate variable",
    help="Limit the update to a climate variable",
    choices=["Temperature", "Precipitation", "Soil Water"],
    type=str,
    required=False,
)
@parameter(
    "limit_frequency",
    name="Frequency",
    help="Limit the update to a specific frequency",
    choices=["Weekly", "Monthly"],
    type=str,
    required=False,
)
@parameter(
    "limit_period",
    name="Period",
    help="Limit the update to a given period",
    type=str,
    required=False,
)
@parameter(
    "limit_year",
    name="Year",
    help="Limit the update to a given year",
    type=str,
    required=False,
)
@parameter(
    "dry_run_only",
    name="Dry run",
    help="No data will be saved in the server",
    type=bool,
    required=False,
    default=True,
)
def push_climate_data(
    import_strategy: str,
    limit_variable: str,
    limit_frequency: str,
    limit_period: str,
    limit_year: str,
    dry_run_only: bool,
):
    """Push climate variables from CDS into DHIS2."""
    s = requests.Session()

    # parse configuration
    with open(
        os.path.join(
            workspace.files_path, "pipelines", "push_climate_data", "config.json"
        )
    ) as f:
        config = json.load(f)

    connection = workspace.dhis2_connection(config["dhis2_connection_slug"])
    if not connection.password:
        raise ConnectionError("No password set for DHIS2 connection")

    s.auth = HTTPBasicAuth(connection.username, connection.password)
    r = s.get(f"{connection.url}/api/me")
    r.raise_for_status()

    username = r.json()["userCredentials"]["username"]
    current_run.log_info(f"Connected to {connection.url} as {username}")
    if not r.json()["access"]["update"]:
        current_run.log_warning(f"User {username} does not have update access")
    if not r.json()["access"]["write"]:
        current_run.log_warning(f"User {username} does not have write access")

    if limit_variable:
        variables = [limit_variable.lower().replace(" ", "_")]
    else:
        variables = ["temperature", "precipitation", "soil_water"]

    # monthly frequency is disabled for now as data elements are not configured
    # in the target dhis2 instance
    if limit_frequency:
        if limit_frequency == "Monthly":
            current_run.log_error("Monthly frequency is temporarily disabled")
            raise ValueError("Monthly frequency is temporarily disabled")
    frequencies = ["weekly"]

    for variable in variables:
        for frequency in frequencies:
            current_run.log_info(f"Preparing {variable} {frequency} data...")

            data_values = prepare_data_values(
                src_file=os.path.join(
                    workspace.files_path, config[f"{variable}_{frequency}"]
                ),
                de_uid=config[f"de_uid_{variable}"],
                coc_uid=config["coc_uid"],
                limit_year=limit_year,
                limit_period=limit_period,
            )

            current_run.log_info(f"Pushing {variable} {frequency} data (dry run)")

            count_dry = push_dry_run(
                session=s,
                api_url=f"{connection.url}/api",
                data_values=data_values,
                import_strategy=import_strategy,
            )

            log_import_summary(
                prefix=f"{variable} {frequency} (dry run)", count=count_dry
            )

            if not dry_run_only:
                count = push(
                    session=s,
                    api_url=f"{connection.url}/api",
                    data_values=data_values,
                    import_strategy=import_strategy,
                    wait=count_dry,
                )

                log_import_summary(prefix=f"{variable} {frequency}", count=count)


@push_climate_data.task
def prepare_data_values(
    src_file: str, de_uid: str, coc_uid: str, limit_year: str, limit_period: str
) -> dict:
    df = pd.read_parquet(src_file)
    df["period"] = df["period"].apply(_fix_period)
    data_values = to_json(
        df=df, de_uid=de_uid, coc_uid=coc_uid, year=limit_year, period=limit_period
    )
    n = sum([len(dv) for dv in data_values.values()])
    current_run.log_info(f"Prepared JSON payload with {n} data values")
    return data_values


@push_climate_data.task
def push_dry_run(
    session: requests.Session,
    api_url: str,
    data_values: dict,
    import_strategy: str = "CREATE",
) -> bool:
    """Push data values to DHIS2 (dry run).

    Parameters
    ----------
    session : Session
        Authenticated requests session
    api_url : str
        DHIS2 API URL
    data_values : dict
        A dict with org units as keys and json formatted data values
        as values
    import_strategy : str, optional
        CREATE, UPDATE, CREATE_AND_UPDATE, or DELETE

    Return
    ------
    dict
        Import counts from import summaries
    """
    count = push_data_values(
        session=session,
        api_url=api_url,
        data_values=data_values,
        import_strategy=import_strategy,
        dry_run=True,
    )
    current_run.log_info(
        f"Dry run: {count['imported']} imported, {count['updated']} updated,"
        f" {count['ignored']} ignored, {count['deleted']} deleted"
    )
    return count


@push_climate_data.task
def log_import_summary(prefix: str, count: dict):
    current_run.log_info(
        f"{prefix}: {count['imported']} imported, {count['updated']} updated,"
        f" {count['ignored']} ignored, {count['deleted']} deleted"
    )


@push_climate_data.task
def push(
    session: requests.Session,
    api_url: str,
    data_values: dict,
    import_strategy: str = "CREATE",
    wait=None,
):
    """Push data values to DHIS2 (dry run).

    Parameters
    ----------
    session : Session
        Authenticated requests session
    api_url : str
        DHIS2 API URL
    data_values : dict
        A dict with org units as keys and json formatted data values
        as values
    import_strategy : str, optional
        CREATE, UPDATE, CREATE_AND_UPDATE, or DELETE
    wait : optional
        Dummy parameter to wait for dry run to finish

    Return
    ------
    dict
        Import counts from import summaries
    """
    count = push_data_values(
        session=session,
        api_url=api_url,
        data_values=data_values,
        import_strategy=import_strategy,
        dry_run=False,
    )
    return count


def _fix_period(period: str) -> str:
    """Remove leading zero before week number."""
    if "W" in period:
        year, week = period.split("W")
        return f"{year}W{str(int(week))}"
    else:
        return period


def to_json(
    df: pd.DataFrame, de_uid: str, coc_uid: str, year: int = None, period: str = None
):
    """Format values in dataframe to JSON.

    Data values are formatted as expected by the dataValueSets
    endpoint of the DHIS2 API. They are divided per org unit:
    output is a dictionary with org unit UID as keys.

    Parameters
    ----------
    df : dataframe
        Input dataframe
    de_uid : str
        Data element UID
    coc_uid : str
        Category option combo UID
    year : int, optional
        Filter data values by year
    period : str, optional
        Limit data values to a given period

    Return
    ------
    dict
        Data values per org unit
    """
    if year:
        df = df[df["period"].apply(lambda x: x.startswith(str(year)))]
    if period:
        df = df[df["period"] == period]

    values = {}

    for ou_uid in df["uid"].unique():
        ou_data = df[df["uid"] == ou_uid]
        values[ou_uid] = []

        for _, row in ou_data.iterrows():
            values[ou_uid].append(
                {
                    "dataElement": de_uid,
                    "orgUnit": ou_uid,
                    "period": row["period"],
                    "value": float(round(row["value"], 2)),
                    "categoryOptionCombo": coc_uid,
                    "attributeOptionCombo": coc_uid,
                }
            )

    return values


def _merge_import_counts(import_counts: List[dict]) -> dict:
    """Merge import counts from import summaries."""
    merge = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}
    for count in import_counts:
        for key in ["imported", "updated", "ignored", "deleted"]:
            merge[key] += count[key]
    return merge


def push_data_values(
    session: requests.Session(),
    api_url: str,
    data_values: dict,
    import_strategy: str = "CREATE",
    dry_run: bool = True,
) -> dict:
    """Push data values using the dataValueSets endpoint.

    Parameters
    ----------
    session : Session
        Authenticated requests session
    api_url : str
        DHIS2 API URL
    data_values : dict
        A dict with org units as keys and json formatted data values
        as values
    import_strategy : str, optional
        CREATE, UPDATE, CREATE_AND_UPDATE, or DELETE
    dry_run : bool, optional
        Whether to save changes on the server or just return the
        import summary

    Return
    ------
    dict
        Import summary
    """
    import_counts = []
    for ou_uid, values in data_values.items():
        r = session.post(
            url=f"{api_url}/dataValueSets",
            json={"dataValues": values},
            params={"dryRun": dry_run, "importStrategy": import_strategy},
        )
        r.raise_for_status()

        summary = r.json()
        if not summary["status"] == "SUCCESS":
            raise DHIS2ImportError(summary["description"])

        import_counts.append(summary["importCount"])
    return _merge_import_counts(import_counts)


if __name__ == "__main__":
    push_climate_data()

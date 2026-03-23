
import numpy as np
import pandas as pd

from stravalib import Client
from oauth import get_athlete
from dotenv import load_dotenv
import os


def extract(client: Client, detailled: bool = False) -> list[dict]:
    """Uses the globally authenticated CLIENT object to extract athlete data."""

    # extract all activities (public + private):
    activities = client.get_activities(limit=None)
    print(f"\nextracting and storing all activites...\n")

    records = []    # store all activities as a list of dicts
    for i, a in enumerate(activities, start=1):

        print(f"\rno. of activities extracted: {i}", end="")

        rec = {
            # general activity metrics:
            "activity_id": a.id,
            "name": a.name,
            "type": a.type,
            "date": a.start_date.date(),                                # UTC date
            "start_time": a.start_date.time(),                          # UTC time only
            "start_date_local": getattr(a, "start_date_local", None),
            # "timezone": getattr(a, "timezone", None),
            "distance_km": a.distance / 1000,                           # m
            "moving_time_s": a.moving_time,                             # s
            "elapsed_time_s": a.elapsed_time,                           # s
            "avg_speed_mps": getattr(a, "average_speed", None),         # m/s
            "max_speed_mps": getattr(a, "max_speed", None),             # m/s
            "total_elev_gain": float(a.total_elevation_gain),           # m
            "highest_elev": getattr(a, "elev_high", None),              # m
            "lowest_elev": getattr(a, "elev_low", None),                # m
            "visibility": getattr(a, "visibility", None),
            "num_comments": getattr(a, "comment_count", None),
            "num_achievements": getattr(a, "achievement_count", None),
            "num_kudos": getattr(a, "kudos_count", None),
            "is_manual": getattr(a, "manual", None),    # auto-recorded vs manually entered activities.
            # running specific metrics:
            "avg_hr": getattr(a, "average_heartrate", None),            # bpm
            "max_hr": getattr(a, "max_heartrate", None),                # bpm
            "avg_cadence_spm": getattr(a, "average_cadence", None),     # spm (strides per minute)
            "gear_id": getattr(a, "gear_id", None),     # use shoe mapping from client.get_athlete().shoes for shoe names
            # note: average_pace not exposed directly by API - compute later as moving_time/distance
        }

        if detailled:
            # extra detailled metrics:
            d = client.get_activity(a.id)
            rec["description"] = getattr(d, "description", None)  # activity description
            rec["calories"] = getattr(d, "calories", None)
            rec["device_name"] = getattr(d, "device_name", None)

        records.append(rec)

    # get statistics on no. of total public activities:
    athlete = client.get_athlete()
    stats = client.get_athlete_stats(athlete.id)
    runs, rides, swims = stats.all_run_totals, stats.all_ride_totals, stats.all_swim_totals
    public_activities = runs.count + rides.count + swims.count

    print(f"\n\n{public_activities}/{len(records)} ({public_activities / len(records) * 100:.1f}% are public)")

    return records


def transform(df: pd.DataFrame, client: Client) -> pd.DataFrame:

    # ------------ UNIT CONVERSIONS ------------ #

    # speed (m/s to km/h and mph)
    df["avg_speed_km_h"] = (df["avg_speed_mps"] * 3.6).round(2)     # m/s -> km/h
    df["max_speed_km_h"] = (df["max_speed_mps"] * 3.6).round(2)
    df["avg_speed_mph"] = (df["avg_speed_mps"] * 2.23694).round(2)  # m/s -> mph
    df["max_speed_mph"] = (df["max_speed_mps"] * 2.23694).round(2)

    # distance (km to miles)
    df["distance_miles"] = (df["distance_km"] * 0.621371).round(2)  # km -> miles

    # time (timedelta objects)
    df["moving_time"] = pd.to_timedelta(df["moving_time_s"], unit="s")      # s -> timedelta
    df["elapsed_time"] = pd.to_timedelta(df["elapsed_time_s"], unit="s")    # s -> timedelta

    # date (datetime objects)
    df["date"] = pd.to_datetime(df["date"])                                 # convert dates to datetime for ordering
    df["start_date_local"] = pd.to_datetime(df["start_date_local"])         # convert to datetime first
    df["end_time_local"] = df["start_date_local"] + df["elapsed_time"]      # compute end datetime

    # optional: extract just the time components (LOCAL) - store as strings:
    df["start_time"] = df["start_date_local"].dt.strftime("%H:%M:%S")  
    df["end_time"] = df["end_time_local"].dt.strftime("%H:%M:%S")

    # ------------ FORMATTING + CLEANING ------------ #

    df["visibility"] = df["visibility"].map({
        "everyone": "Everyone",
        "followers_only": "Followers Only",
        "only_me": "Only Me",
    })

    # map the gear IDs to the shoe name:
    athlete = client.get_athlete()
    shoe_mapping = {}
    for gear in athlete.shoes:
        shoe_mapping[gear.id] = gear.name
    df["shoe_used"] = df["gear_id"].map(shoe_mapping)

    # clean the activity "type" column, RelaxedActivityType:
    df["type"] = df["type"].astype(str).str.extract(r"root='([^']+)'")  # any character except ', match 1+

    # average running cadence (only runs are doubled as it's per foot initially):
    df.loc[df["type"] == "Run", "avg_cadence_spm"] *= 2

    # ------ DERIVED METRICS ------ #

    # pace (as time deltas):
    for speed_col, pace_col in zip(
        ["avg_speed_km_h", "max_speed_km_h", "avg_speed_mph", "max_speed_mph"],
        ["avg_pace_km", "max_pace_km", "avg_pace_mile", "max_pace_mile"]
    ):
        # mask zeros to avoid zero-division errors:
        df.loc[df[speed_col] <= 0, speed_col] = np.nan

        # create new pace column (converts to min/km and min/mile):
        df[pace_col] = pd.to_timedelta(1 / df[speed_col] * 60, unit="min", errors="coerce")

    return df.round(2)  # all numerics to 2 d.p.


def _merge_additional_data(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Merge extra additional data not available from the API with pagination (using manual file download from Strava)."""
    
    # activities CSV should be downloaded manually from Strava
    df2 = pd.read_csv(filename)    

    # only keep columns that weren't extracted with the API
    columns = [
        "Activity ID", "Activity Description", 
        "Relative Effort", "Max Grade", "Calories", 
        "Average Temperature", "Humidity", 
        "Wind Speed", "Media"
    ]

    df2 = df2[columns]

    # use Activity ID as the foreign key for merging:
    x = pd.merge(
        left=df,
        right=df2,
        left_on="activity_id",
        right_on="Activity ID",
        how="left"  # left join
    )

    x = x.drop(columns=["Activity ID"], axis=1)

    return x.rename(columns={
        "Activity Description": "desc",
        "Relative Effort": "relative_effort",
        "Max Grade": "max_grade",
        "Calories": "calories",
        "Average Temperature": "avg_temp",
        "Humidity": "humidity",
        "Wind Speed": "wind_speed",
        "Media": "media"   # requires cleaning later for counts
        }
    )


def export(
    client: Client,
    filename: str = "data/strava_activities.parquet"
) -> None:

    # obtains all records for strava athlete
    records = extract(client)

    # convert records into a DataFrame object
    df = pd.DataFrame(records)

    # clean, transform and prepare DataFrame
    df = transform(df, client)

    # export DataFrame to a Parquet file
    (
        df 
        .sort_values(by="date", ascending=True)
        .reset_index(drop=True)
        .to_parquet(filename, index=False)
    )


def describe_matrix(df: pd.DataFrame) -> pd.DataFrame:

    df = df[df["type"] == "Run"]

    df = df.describe().round(2).drop(columns=["activity_id"], axis=1)

    # time values:
    for left, right in zip(
        ["moving_time_s", "elapsed_time_s"],
        ["moving_time", "elapsed_time"]
    ):
        df[right] = df[left].apply(
            lambda x: (
                f"{int(x // 3600):02d}:"
                f"{int((x % 3600) // 60):02d}:"
                f"{int(x % 60):02d}"
            )
        )

    # average running pace:
    for speed_col, pace_col, unit in zip(
        ["avg_speed_km_h", "max_speed_km_h", "avg_speed_mph", "max_speed_mph"],
        ["avg_pace_km", "max_pace_km", "avg_pace_mile", "max_pace_mile"],
        ["km", "km", "mile", "mile"]
    ):
        df[pace_col] = (
            pd.to_timedelta(1 / df[speed_col] * 60, unit="min", errors="coerce")
            .apply(
                lambda x: (
                    f"{int((x.total_seconds() % 3600) // 60):02d}:"
                    f"{int(x.total_seconds() % 60):02d}"
                    f" min/{unit}"
                )
            )
        )

    # formatting
    for col in ["avg_hr", "max_hr"]:
        df[col] = df[col].apply(lambda x: f"{x:.0f} bpm")

    # cadence - add units
    df["avg_cadence_spm"] = df["avg_cadence_spm"].apply(lambda x: f"{x:.0f} spm")

    # speeds - add km and miles units:
    for mile_col, km_col, unit in zip(
        ["avg_speed_mph", "avg_speed_km_h"],
        ["max_speed_mph", "max_speed_km_h"],
        ["mph", "km/h"]
    ):
        df[km_col] = df[km_col].apply(lambda x: f"{x:.2f} {unit}")
        df[mile_col] = df[mile_col].apply(lambda x: f"{x:.2f} {unit}")

    # elevations - add comma and units
    for elev_col in ["total_elev_gain", "lowest_elev", "highest_elev"]:
        df[elev_col] = df[elev_col].apply(lambda x: f"{x:,.0f} m")

    # distances - add units
    for dist_col, unit in zip(
        ["distance_km", "distance_miles"],
        ["km", "mi"]
    ):
        df[dist_col] = df[dist_col].apply(lambda x: f"{x:.1f} {unit}")

    # ordering of columns to include:
    columns = [
        "distance_km", "distance_miles",
        "moving_time", "elapsed_time",
        "avg_pace_km", "avg_pace_mile",
        "avg_cadence_spm",
        # "max_pace_km", "max_pace_mile",
        "avg_hr", "max_hr",
        "total_elev_gain", "lowest_elev", "highest_elev",
        "num_comments", "num_achievements",  "num_kudos",
        "avg_speed_mph", "avg_speed_km_h",
        # "max_speed_mph", "max_speed_km_h",
        # columns not included from API:
        # "num_photos",
        # "relative_effort",
        # "max_grade",
        # "calories",
        # "wind_speed",
        # "avg_temp",
        # "humidity"
    ]

    return (
        df[columns]
        .transpose()
        .reset_index(names="metric")
        .drop("count", axis=1)
        # drop percentiles:
        .drop(columns=["25%", "50%", "75%"], axis=1)
        # re-order columns:
        .loc[:, ["metric", "mean", "min", "max", "std"]]
    )


def main() -> None:

    load_dotenv()   # parses environment variables from .env file

    # create a Strava client (athlete)
    client = get_athlete(
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"), 
        refresh_token=os.getenv("REFRESH_TOKEN2"),
        view_tokens=False,
        verbose=False
    )

    export(client)

    df = pd.read_parquet("data/strava_activities.parquet")

    desc = describe_matrix(df)

    print(f"\ndisplaying describe matrix:\n")
    print(desc)


if __name__ == "__main__":
    main()
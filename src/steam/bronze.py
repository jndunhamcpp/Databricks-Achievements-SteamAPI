import requests
import sys
import random
import time
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.functions import desc
from src.steam.client import SteamClient
from src.config.settings import (
    FLUSH_EVERY,
    STEAM_API_KEY,
    STEAM_BASE_URL,
    STEAM_MAX_CALLS
)

def pull_games_steamspy(spark):
    """
    Pull all games from Steam in the form ["appid", "game_name"]
    from the SteamSpy API.
    """

    page = 0
    while True:
        print(f"Fetching page {page}...")

        resp = requests.get(
            "https://steamspy.com/api.php",
            params={"request": "all", "page": page},
            timeout=60
        )

        if not resp.text or not resp.text.strip().startswith("{"):
            print("Invalid response, stopping.")
            break

        data = resp.json()
        if not data:
            print("Empty page, stopping.")
            break

        rows = []
        for appid, payload in data.items():
            payload["appid"] = int(appid)
            payload["steamspy_page"] = page
            rows.append(Row(**payload))

        df = spark.createDataFrame(rows)

        (
            df.write
              .format("delta")
              .mode("append")
              .saveAsTable("bronze.steamspy")
        )

        page += 1
        time.sleep(60) #SteamSpy API limit

def pull_player_count_steam(spark):
    """
    Pull current player count using Steam API for every appid in
    bronze.steamspy. This player count is more accurate
    than anything that SteamSpy can give us
    """

    daily_count = 0

    steam_client = SteamClient(
        base_url=STEAM_BASE_URL,
        api_key=STEAM_API_KEY,
    )

    apps_df = (
        spark.table("bronze.steamspy")
        .select("appid", "name")
        .where("appid IS NOT NULL AND name IS NOT NULL")
    )

    rows = []

    for idx, row in enumerate(apps_df.toLocalIterator(), start=1):
        appid = int(row["appid"])
        name = row["name"]

        if daily_count >= STEAM_MAX_CALLS:
            print("Reached Steam daily API call limmit; stopping.")
            break

        player_count = None
        try:
            player_count = steam_client.get_number_of_current_players(appid)
            daily_count += 1
        except Exception as e:
            print(f"Failed to fetch player count for appid {appid}: {e}")

        rows.append((appid, name, player_count))

        if len(rows) >= FLUSH_EVERY:
            (
                spark.createDataFrame(
                    rows,
                    ["appid", "name", "player_count"]
                )
                .write
                .mode("append")
                .format("delta")
                .saveAsTable("bronze.player_count")
            )

            print(f"Flushed {len(rows)} rows at record {idx}")
            rows.clear()

    # Flush remainder
    if rows:
        (
            spark.createDataFrame(
                rows,
                ["appid", "name", "player_count"]
            )
            .write
            .mode("append")
            .format("delta")
            .saveAsTable("bronze.player_count")
        )

def pull_global_achievements_steam(spark):
    """
    For every appid in bronze.player_count, pull
    every achievement and completion percentage from
    Steam API
    """

    daily_count = 0

    steam_client = SteamClient(
        base_url=STEAM_BASE_URL,
        api_key=STEAM_API_KEY,
    )

    apps_df = (
        spark.table("bronze.player_count")
        .select("appid", "name", "player_count")
        .where("appid IS NOT NULL")
        .orderBy(desc("player_count"))
    )

    rows = []
    for idx, row in enumerate(apps_df.toLocalIterator(), start=1):
        appid = int(row["appid"])
        game_name = row["name"]
        player_count = row["player_count"]

        if daily_count >= STEAM_MAX_CALLS:
            print("Reached Steam daily API call limmit; stopping.")
            break

        achievements = []
        try:
            achievements = steam_client.get_global_achievements(appid)
            daily_count += 1
        except Exception as e:
            print(f"Failed to fetch achievements for appid {appid}: {e}")


        for ach in achievements:
            achievement_name = ach.get("name")
            percentage = ach.get("percent")

            rows.append(
                (
                    appid,
                    game_name,
                    achievement_name,
                    float(percentage) if percentage is not None else None,
                    player_count,
                )
            )

        if len(rows) >= FLUSH_EVERY:
            (
                spark.createDataFrame(
                    rows,
                    [
                        "appid",
                        "game_name",
                        "achievement_name",
                        "percentage",
                        "player_count",
                    ],
                )
                .write
                .mode("append")
                .format("delta")
                .saveAsTable("bronze.global_achievements")
            )

            print(f"Flushed {len(rows)} rows at app {idx}")
            rows.clear()

    if rows:
        (
            spark.createDataFrame(
                rows,
                [
                    "appid",
                    "game_name",
                    "achievement_name",
                    "percentage",
                    "player_count",
                ],
            )
            .write
            .mode("append")
            .format("delta")
            .saveAsTable("bronze.global_achievements")
        )


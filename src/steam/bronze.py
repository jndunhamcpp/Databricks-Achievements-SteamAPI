import requests
import sys
import random
import time
from pyspark.sql import functions as F
from pyspark.sql import Row
from src.steamspy.client import SteamSpyAPI
from src.steam.client import SteamClient
from src.config.settings import (
    BRONZE_FLUSH_EVERY,
    STEAM_API_KEY,
    STEAM_BASE_URL,
)

def pull_games_steampy(spark):
    all_apps = {}
    page = 0

    while True:
        print(f"Fetching page {page}...")

        resp = requests.get(
            "https://steamspy.com/api.php",
            params={
                "request": "all",
                "page": page
            },
            timeout=60
        )
        if not resp.text or not resp.text.strip().startswith("{"):
            print(f"Stopping at page {page}: non-JSON response")
            break

        try:
            data = resp.json()
        except ValueError:
            print(f"Stopping at page {page}: JSON decode failed")
            break

        if not data:
            print(f"No data at page {page}, stopping.")
            break

        all_apps.update(data)
        print(f"Total apps so far: {len(all_apps)}")

        page += 1
        time.sleep(60)  # SteamSpy rate limit for 'all'

    len(all_apps)

    rows = [
        (int(v["appid"]), v["name"])
        for v in all_apps.values()
        if v.get("name")
    ]

    apps_df = spark.createDataFrame(rows, ["appid", "game_name"])
    apps_df.count()

    apps_df.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable("bronze.steamspy")


def pull_achievements(spark):
    """
    Pull global achievement percentages + current player counts
    for all Steam apps and store them in bronze.steam_global_achievements
    """

    steam_client = SteamClient(
        base_url=STEAM_BASE_URL,
        api_key=STEAM_API_KEY,
    )

    apps = (
        spark.table("bronze.steamspy")
        .select("appid", "game_name")
        .collect()
    )

    rows = []
    total_apps = len(apps)

    for idx, app in enumerate(apps, start=1):
        appid = app.appid
        game_name = app.game_name

        try:
            player_count = steam_client.get_number_of_current_players(appid)
            achievements = steam_client.get_global_achievements(appid)

            for ach in achievements:
                rows.append(
                    Row(
                        appid=appid,
                        game_name=game_name,
                        achievement_name=ach.get("name"),
                        percent=float(ach.get("percent", 0.0)),
                        player_count=player_count,
                    )
                )

        except Exception as e:
            print(f"Failed achievements for appid {appid}: {e}")

        if len(rows) >= BRONZE_FLUSH_EVERY:
            spark.createDataFrame(rows) \
                .write \
                .mode("append") \
                .format("delta") \
                .saveAsTable("bronze.steam_global_achievements")

            print(f"Flushed {len(rows)} rows at app {idx}/{total_apps}")
            rows.clear()

        time.sleep(0.2)

    if rows:
        spark.createDataFrame(rows) \
            .write \
            .mode("append") \
            .format("delta") \
            .saveAsTable("bronze.steam_global_achievements")

    print(f"Finished ingesting achievements for {total_apps} apps")


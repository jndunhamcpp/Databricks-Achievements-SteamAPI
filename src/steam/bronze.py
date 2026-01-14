import requests
import sys
import random
import time
from pyspark.sql import functions as F
from src.steamspy.client import SteamSpyAPI
from src.steam.client import SteamClient
from src.config.settings import (
    BRONZE_FLUSH_EVERY,
    STEAM_API_KEY,
    STEAM_BASE_URL,
    MIN_ACHIEVEMENTS
)


def ingest_steam_apps(spark):
    steam = SteamClient(
        base_url=STEAM_BASE_URL,
        api_key=STEAM_API_KEY
    )

    all_apps = {}
    page = 0

    # Call SteamSpy API
    while True:
        print(f"Fetching page {page}...")

        try:
            data = steamspy.get_all_apps_page(page)
        except ValueError as e:
            print(f"Stopping at page {page}: {e}")
            break

        if not data:
            print(f"No data at page {page}, stopping.")
            break

        all_apps.update(data)
        print(f"Total apps so far: {len(all_apps)}")

        page += 1
        time.sleep(60)  # SteamSpy rate limit for 'all'


    rows = [
        (int(v["appid"]), v["name"])
        for v in all_apps.values()
        if v.get("name")
    ]
    random.shuffle(rows)
    apps_df = spark.createDataFrame(rows, ["appid", "game_name"])

    qualified_games = []

    for idx, (app_id, name) in enumerate(rows, start=1):
        try:
            count = steam.get_achievement_count(app_id)
            if count >= MIN_ACHIEVEMENTS:
                qualified_games.append((app_id, name))
            time.sleep(0.05)
        except Exception:
            continue

    games_df = spark.createDataFrame(
        qualified_games,
        ["AppId", "GameName"]
    )
    games_df.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable("bronze.all_games")


def ingest_global_achievements(spark):
    steam = SteamClient(
        base_url=STEAM_BASE_URL,
        api_key=STEAM_API_KEY
    )

    games = (
        spark.read.table("bronze.all_games")
        .select("AppId", "GameName")
        .collect()
    )

    buffer = []
    for idx, game in enumerate(games, start=1):
        app_id = game["AppId"]
        game_name = game["GameName"]

        print(f"[{idx}/{len(games)}] Processing AppId={app_id}, GameName='{game_name}'")

        try:
            achievements = steam.get_global_achievements(app_id)
            if not achievements:
                continue

            buffer.extend([
                (app_id, game_name, a["name"], float(a["percent"]))
                for a in achievements
            ])

            if idx % BRONZE_FLUSH_EVERY == 0:
                spark.createDataFrame(
                    buffer,
                    ["AppId", "GameName", "AchievementName", "GlobalPercentage"]
                ).write.mode("append").format("delta") \
                 .saveAsTable("bronze.all_games_achievements")

                buffer.clear()

            time.sleep(0.1)

        except Exception as e:
            print(f"  -> ERROR processing AppId={app_id}: {e}")
            continue
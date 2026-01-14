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

def _flush_bronze_achievements(spark, rows):
    spark.createDataFrame(
        rows,
        ["AppId", "GameName", "AchievementName", "GlobalPercentage", "PlayerCount"]
    ).write.mode("append").format("delta") \
     .saveAsTable("bronze.global_achievements")

def ingest_steam_apps():
    steam = SteamClient(
        base_url=STEAM_BASE_URL,
        api_key=STEAM_API_KEY
    )

    steamspy = SteamSpyAPI()

    all_apps = {}
    page = 0

    while True:
        print(f"Fetching page {page}...")
        data = steamspy.get_all_apps_page(page)

        if not data:
            break

        all_apps.update(data)
        page += 1
        time.sleep(60)

    rows = []

    for v in all_apps.values():
        if not v.get("name"):
            continue

        app_id = int(v["appid"])
        game_name = v["name"]

        try:
            playercount = steam.get_number_of_current_players(app_id)
        except Exception:
            playercount = 0

        rows.append((app_id, game_name, playercount))

    random.shuffle(rows)
    return rows

def ingest_global_achievements(spark, app_rows):
    steam = SteamClient(
        base_url=STEAM_BASE_URL,
        api_key=STEAM_API_KEY
    )

    buffer = []

    for idx, (app_id, game_name, playercount) in enumerate(app_rows, start=1):
        print(f"[{idx}] AppId={app_id}, GameName={game_name}")

        try:
            achievements = steam.get_global_achievements(app_id)
            if not achievements:
                continue

            buffer.extend([
                (app_id, game_name, a["name"], float(a["percent"]), playercount)
                for a in achievements
            ])

            if idx % BRONZE_FLUSH_EVERY == 0:
                _flush_bronze_achievements(spark, buffer)
                buffer.clear()

            time.sleep(0.1)

        except Exception:
            continue

    if buffer:
        _flush_bronze_achievements(spark, buffer)
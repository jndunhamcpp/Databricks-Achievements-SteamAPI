import requests
import sys
import random
import time
from pyspark.sql import functions as F
from src.steam.client import SteamClient
from src.config.settings import (
    BRONZE_FLUSH_EVERY,
    STEAM_API_KEY,
    STEAM_BASE_URL,
    MIN_ACHIEVEMENTS
)

def steamspy(spark):
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

def steamapi(spark):
    steam = SteamClient(
        base_url=STEAM_BASE_URL,
        api_key=STEAM_API_KEY
    )

    # shuffle BEFORE Spark
    random.shuffle(rows)

    # --- Create Spark DataFrame (optional persistence) ---
    apps_df = spark.createDataFrame(rows, ["appid", "game_name"])

    # --- Driver-side filtering with Steam API ---
    qualified_games = []

    for idx, (app_id, name) in enumerate(rows, start=1):
        try:
            count = steam.get_achievement_count(app_id)
            if count >= MIN_ACHIEVEMENTS:
                qualified_games.append((app_id, name))
            time.sleep(0.05)
        except Exception:
            continue

    # --- Persist results ---
    games_df = spark.createDataFrame(
        qualified_games,
        ["AppId", "GameName"]
    )

    games_df.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable("bronze.games_sample")
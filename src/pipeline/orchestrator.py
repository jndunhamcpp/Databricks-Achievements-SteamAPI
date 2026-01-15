from pyspark.sql import SparkSession
from src.bronze.steamspy import pull_games_steampy
from src.bronze.achievements import pull_achievements
from src.silver.global_achievements import build_silver_global_achievements
from src.gold.global_achievements import (
    build_gold_app_achievement_summary,
    build_gold_hardest_achievements,
    build_gold_completion_friendly_games,
)


def run_pipeline(spark: SparkSession):

    # 1. Setup
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
    print("Setup complete")

    # 2. Bronze ingestion
    print("Starting Bronze ingestion...")
    pull_games_steampy(spark)
    pull_achievements(spark) 
    print("Bronze ingestion complete")

    # 3. Silver transformation
    print("Starting Silver transformation...")
    build_silver_global_achievements(spark)
    print("Silver transformation complete")

    # 4. Gold transformations
    print("Starting Gold transformations...")
    build_gold_app_achievement_summary(spark)
    build_gold_hardest_achievements(spark)
    build_gold_completion_friendly_games(spark)
    print("Gold transformations complete")
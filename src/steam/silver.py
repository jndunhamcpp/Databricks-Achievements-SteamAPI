from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from src.config.settings import (
    MIN_ACHIEVEMENTS,
    MIN_CURRENT_PLAYERS
)

def build_silver_global_achievements(spark: SparkSession):
    """
    Silver transformation for global achievements.

    Rules:
    1. Remove rows with PlayerCount <= MIN_CURRENT_PLAYERS
    2. Remove games that have fewer than MIN_ACHIEVEMENTS achievements
    """

    # Read bronze table
    bronze_df = spark.table("bronze.global_achievements")

    filtered_players_df = bronze_df.filter(
        F.col("PlayerCount") > MIN_CURRENT_PLAYERS
    )

    achievement_counts_df = (
        filtered_players_df
        .groupBy("AppId")
        .agg(F.count("*").alias("achievement_count"))
        .filter(F.col("achievement_count") >= MIN_ACHIEVEMENTS)
        .select("AppId")
    )

    silver_df = (
        filtered_players_df
        .join(achievement_counts_df, on="AppId", how="inner")
    )

    silver_df.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable("silver.global_achievements")
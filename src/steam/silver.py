from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from src.config.settings import (
    MIN_ACHIEVEMENTS,
    MIN_CURRENT_PLAYERS
)

def build_silver_global_achievements(spark: SparkSession):
    """
    Silver transformation for bronze.global_achievements.

    Rules:
    1. Remove rows with player_count <= MIN_CURRENT_PLAYERS
    2. Remove games that have fewer than MIN_ACHIEVEMENTS achievements
    """

    bronze_df = spark.table("bronze.global_achievements")

    filtered_players_df = bronze_df.filter(
        F.col("player_count") > MIN_CURRENT_PLAYERS
    )

    achievement_counts_df = (
        filtered_players_df
        .groupBy("appid")
        .agg(F.count("*").alias("achievement_count"))
        .filter(F.col("achievement_count") >= MIN_ACHIEVEMENTS)
        .select("appid")
    )

    silver_df = (
        filtered_players_df
        .join(achievement_counts_df, on="appid", how="inner")
    )

    silver_df.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable("silver.global_achievements")

import json
import time
import requests


class SteamClient:
    """
    Steam Web API client
    """
    def __init__(self, base_url: str, api_key: str = None, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def _get(self, endpoint: str, params: dict) -> dict:
        """
        Internal GET request handler
        """
        if self.api_key:
            params["key"] = self.api_key

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.get(url, params=params, timeout=self.timeout)

        response.raise_for_status()
        return response.json()

    def get_global_achievements(self, app_id: int) -> list[dict]:
        """
        Fetch global achievement percentages for an app
        """
        payload = self._get(
            endpoint="ISteamUserStats/GetGlobalAchievementPercentagesForApp/v2/",
            params={"gameid": app_id}
        )

        achievements = (
            payload
            .get("achievementpercentages", {})
            .get("achievements", [])
        )

        return achievements
    
    def get_number_of_current_players(self, app_id: int) -> int:
        """
        Fetch the current number of players for a Steam app.
        Returns 0 if unavailable.
        """
        try:
            payload = self._get(
                endpoint="ISteamUserStats/GetNumberOfCurrentPlayers/v1/",
                params={"appid": app_id}
            )

            return int(payload.get("response", {}).get("player_count", 0))

        except Exception:
            return 0
    
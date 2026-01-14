import requests
import time


class SteamSpyAPI:
    def __init__(self, base_url="https://steamspy.com/api.php", timeout=60):
        self.base_url = base_url
        self.timeout = timeout

    def get_all_apps_page(self, page: int) -> dict:
        """
        Fetch one page from SteamSpy 'all' endpoint.
        Returns a dict keyed by appid.
        """
        resp = requests.get(
            self.base_url,
            params={
                "request": "all",
                "page": page
            },
            timeout=self.timeout
        )

        if not resp.text or not resp.text.strip().startswith("{"):
            raise ValueError("Non-JSON response from SteamSpy")

        try:
            return resp.json()
        except ValueError as e:
            raise ValueError("Failed to decode SteamSpy JSON") from e
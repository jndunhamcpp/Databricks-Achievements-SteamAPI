import requests
import time


class SteamSpyAPI:
    def __init__(self, base_url="https://steamspy.com/api.php", timeout=60):
        self.base_url = base_url
        self.timeout = timeout

    def get_all_apps_page(self, page: int) -> dict | None:
        try:
            resp = requests.get(
                self.base_url,
                params={"request": "all", "page": page},
                timeout=self.timeout
            )
        except ReadTimeout:
            return None
        except RequestException:
            return None

        if not resp.text or not resp.text.strip().startswith("{"):
            return None

        try:
            return resp.json()
        except ValueError:
            return None
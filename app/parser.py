from itertools import cycle
from random import randint
from typing import Any, Literal

import requests
from tenacity import retry
from tenacity.retry import retry_if_result
from tenacity.stop import stop_after_attempt

from app.config import PROXY_LIST, TOR_PORT
from app.main import logger


def skip_request(*args) -> bool:
    return False


def validate_result(result: Any) -> bool:
    """Проверка результата запроса."""
    return isinstance(result, bool)


class Parser:
    """Парсер сайта http://epal.gg с поддержкой GET и POST методов через одну и ту же сессию."""

    BASE_URL: str = "https://www.epal.gg"
    HEADERS: dict[str, dict[str, str]] = {
        "POST": {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en",
            "content-type": "application/json",
            "sec-ch-ua": '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "Referer": "https://www.epal.gg/",
            "Referrer-Policy": "strict-origin-when-cross-origin",
        },
        "GET": {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "max-age=0",
            "sec-ch-ua": '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "cookie": "_gcl_au=1.1.1339537856.1663137020; gr_user_id=dd796919-1d82-4d00-a177-587022f307f5; G_ENABLED_IDPS=google; _gid=GA1.2.1250243474.1663533214; a39dfcf2ec1add0d_gr_session_id=41f882e7-f3d0-4b07-b2f0-5fd38c256d31; a39dfcf2ec1add0d_gr_session_id_41f882e7-f3d0-4b07-b2f0-5fd38c256d31=true; _ga_XNBS01PRK4=GS1.1.1663602041.12.1.1663602041.60.0.0; _ga_E78FQ3CRGE=GS1.1.1663602041.12.0.1663602041.0.0.0; lng=en-US; _ga=GA1.1.899984894.1663137020; _ga_FF63LKSL16=GS1.1.1663602043.12.0.1663602045.0.0.0",
        },
    }

    def __init__(self, packet_proxy: bool = False) -> None:
        """Инициализация сессии."""
        self.session = requests.Session()
        self.packet_proxy = packet_proxy
        if packet_proxy:
            self.proxy_pool = cycle(PROXY_LIST)

    @retry(
        stop=stop_after_attempt(5),
        retry_error_callback=skip_request,
        retry=retry_if_result(validate_result),
    )
    def request(
        self, method: Literal["POST", "GET"], path: str, **kwargs
    ) -> requests.Response | bool:
        """Запрос к сайту."""
        if path.startswith("/"):
            path = self.BASE_URL + path

        thread_name = ""
        if value := kwargs.get("logger_thread_info"):
            thread_name = value + " "

        try:
            response = self.session.request(
                method, path, headers=self.HEADERS[method], **kwargs
            )
        except requests.exceptions.ConnectionError:
            logger.error(f"{thread_name}requests.exceptions.ConnectionError")
            return False

        # logger.info(
        #     f"{thread_name}{method} request to {path} with data {kwargs.get('data')}. Status code: {response.status_code}"
        # )

        if response.status_code != 200 or (
            method == "POST" and response.json()["content"] is None
        ):
            self.update_proxy()
            # logger.error(f"{thread_name}Request to {path} with data {kwargs.get('data')} filed.")
            return False

        return response

    def update_proxy(self) -> None:
        """Смена прокси сессии в зависимости от настроек."""
        if self.packet_proxy:
            proxy = self.new_proxy()
        else:
            proxy = self.gen_proxy()
        self.session.proxies.update(proxy)

    def gen_proxy(self) -> dict[str, str]:
        """Новый прокси через тор."""
        proxy_auth = f"{randint(1, 0x7FFFFFFF)}:{str(randint(1, 0x7FFFFFFF))}"
        proxies = {
            "http": f"socks5://{proxy_auth}@localhost:{TOR_PORT}",
            "https": f"socks5://{proxy_auth}@localhost:{TOR_PORT}",
        }
        return proxies

    def new_proxy(self) -> dict[str, str]:
        """Новый прокси из self.proxy_pool."""
        pr = f"http://{next(self.proxy_pool)}"
        proxies = {
            "http": pr,
            "https": pr,
        }
        return proxies

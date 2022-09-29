import json
import re
from pathlib import Path
from queue import Queue
from datetime import datetime
from typing import Any, Generator, Optional

import pandas as pd
from pymongo.collection import Collection

from app.config import OUTPUT_EXCEL_PATH
from app.main import logger
from app.parser import Parser
from app.pydantic_classes import Game, Games


def clean_gamer_json(data: str) -> str:
    """Очистка json от невалидируемых объектов."""
    data = re.sub("undefined", '"undefined"', data)
    data = re.sub(r"function\(\){}", '"function(){}"', data)
    data = re.sub(r"\"create\".+{}}\)\)}", "", data)
    data = re.sub(r"\"getLastCreatedEntityKey.+r}},", "", data)
    data = re.sub(r" \"blockMap\": ", "", data + "}")
    return data


def parse_page(url: str, parser: Parser) -> dict[str, str | int] | Exception:
    """Извлечение json-а из страницы по ссылке."""
    page_content = parser.request(method="GET", path=url).text
    dirty_json = re.search(r"Props = {.+}", page_content)
    if dirty_json is None:
        return Exception("Don't have needed json.")
    dirty_json = dirty_json.group()[8:]
    gamer_json = clean_gamer_json(dirty_json)
    data = json.loads(gamer_json)
    return data


def parse_categories_id(parser: Parser) -> list[Game] | Exception:
    """Парсинг id категорий."""
    data = parse_page("https://www.epal.gg/epals/valorant-lfg", parser)
    if isinstance(data, Exception):
        return data
    games = Games.parse_obj(data["global"])
    return games.games


def parse_user_id_by_category(product_type_id: int, parser: Parser) -> list[int]:
    """Париснг всех userId из категории."""
    data = {
        "ps": 20,
        "orderField": 1,
        "productTypeId": product_type_id,
        "pn": 1,
        "clientNo": "d57ff9454",
    }
    url = "https://play.epal.gg/web/product-search/list"
    page_counter = 1
    content_length = 20
    users_id = []
    error_counter = 0
    while content_length == 20 and error_counter < 5 and page_counter <= 500:
        data["pn"] = page_counter
        response = parser.request("POST", url, data=str(data))
        if response == False:
            error_counter += 1
            continue
        try:
            response = response.json()
            if response["status"] == "ERROR":
                error_counter += 1
                continue
            users = response["content"]
            content_length = len(users)
            users_id.extend(user["userId"] for user in users)
            error_counter = 0
        except:
            logger.error("Пользователи не считались из категории.")
        page_counter += 1
    return users_id


def write_gamers_data_to_file(collection: Collection) -> None:
    """Загрузка свежеспаршенных данных в файл."""
    data = load_today_data(collection)
    data.to_csv(f"{OUTPUT_EXCEL_PATH}/{str(datetime.now().date())}.csv")


def load_today_data_from_db(collection: Collection) -> pd.DataFrame:
    """Загрузка свежеспаршенных данных в DataFrame."""
    data = pd.DataFrame(gamer_collection.find({"date":str(datetime.now().date())}))
    return data

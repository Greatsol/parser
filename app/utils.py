import json
import re
from typing import Any, Generator

import pandas as pd

from app.config import FILES_PATH
from app.parser import Parser
from app.pydantic_classes import Game, Games
from app.main import logger


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
        "clientNo": "d57ff9454"
    }
    url = "https://play.epal.gg/web/product-search/list"
    page_counter = 1
    content_length = 20
    users_id = []
    while content_length == 20:
        data["pn"] = page_counter
        response = parser.request("POST", url, data=str(data))
        try:
            users = response.json()["content"]
            content_length = len(users)
            users_id.extend(user["userId"] for user in users)
        except:
            logger.error("Пользователи не считались из категории.")
        page_counter += 1
    return users_id


def func_chunks_generators(lst: list[Any], n: int) -> Generator:
    """Разделение массива ссылок на n равных частей."""
    chunk_len = int(len(lst) / n)
    for i in range(0, len(lst), chunk_len):
        yield lst[i: i + chunk_len]


def write_gamers_data_to_file(data: tuple[dict[str, Any]], file_name: str) -> None:
    """Запись категории игроков в файл."""
    pd.DataFrame(data).to_excel(f"{FILES_PATH}{file_name}.xlsx")


def write_last_file(q: Queue) -> None:
    """Запись остатков данных в очереди в файл."""
    data = []
    while q.qsize():
        data.append(q.get())
    write_gamers_data_to_file(data, name="final")

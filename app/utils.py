import json
import re
from queue import Queue
from typing import Any, Generator, Optional
from pathlib import Path

import pandas as pd

from app.config import FILES_PATH, JSON_PATH
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


def func_chunks_generators(lst: list[Any], n: int) -> Generator:
    """Разделение массива ссылок на n равных частей."""
    chunk_len = int(len(lst) / n)
    for i in range(0, len(lst), chunk_len):
        yield lst[i: i + chunk_len]


def write_gamers_data_to_file(data: list[dict[str, Any]], file_name: int | str) -> None:
    """Запись категории игроков в файл."""
    pd.DataFrame(data).to_excel(f"{FILES_PATH}{file_name}.xlsx")


def write_last_file(q: Queue) -> None:
    """Запись остатков данных в очереди в файл."""
    data = []
    while q.qsize():
        data.append(q.get())
    write_gamers_data_to_file(data, file_name="final")


def write_ids_to_json(data: set[int]) -> None:
    """Записать все id в json для продолжения парсинга"""
    with open(JSON_PATH, "w", encoding="utf8") as file:
        json.dump(list(data), file, indent=4)


def cocncat_xlsxs(file_path: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Объединяет все xlsx в FILE_PATH."""
    dfs = []
    for path in Path(FILES_PATH).iterdir():
        try:
            dfs.append(pd.read_excel(path))
        except pd._config.config.OptionError:
            logger.error(f"Error with reading file: {path}")
    df = pd.concat(dfs)
    if file_path:
        df.to_excel(file_path)
    else:
        return df


def parse_used_id_from_xlsx() -> set[int]:
    """Возвращает список использованных id."""
    return set(cocncat_xlsxs().user_id)

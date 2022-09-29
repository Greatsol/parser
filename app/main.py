from queue import Queue
from threading import Thread
from typing import Any

from loguru import logger
from pymongo import MongoClient

from app.config import CONTINUE_PARSING, DB_URI, THREADS_NUM
from app.parse_types import Gamer
from app.parser import Parser
from app.pydantic_classes import Game
from app.utils import (load_today_data_from_db, parse_categories_id,
                       parse_user_id_by_category, write_gamers_data_to_file)


logger.add(
    "log/debug.log",
    format="{time} | {level} | {message}",
    level="INFO",
    rotation="12:00",
    compression="zip",
)


"""Подключение к БД."""
client = MongoClient(DB_URI)
db = client["epal_db"]
gamer_collection = db["Gamers"]


USERS_ID_DATA: list[int] | set[int] = []
CATEGORY_QUEUE: Queue[Game] = Queue()
MAIN_QUEUE: Queue[int] = Queue()
USER_DATA_QUEUE: Queue[dict[str, Any]] = Queue()


def parse_id_multithread() -> None:
    """Парсит id игроков из всех категорий в многопоточном режиме."""
    games = parse_categories_id(parser=Parser())
    if isinstance(games, Exception):
        raise games

    for game in games:
        CATEGORY_QUEUE.put(game)

    category_threads = [
        Thread(
            target=parse_id_thread,
            kwargs={"name": f"Категории {i+1}"},
            name=f"Категории {i+1}",
        )
        for i in range(THREADS_NUM)
    ]
    [thread.start() for thread in category_threads]
    [thread.join() for thread in category_threads]


@logger.catch
def parse_id_thread(name: str) -> None:
    """Поток парсинга id игороков из категорий."""
    logger.info(f"Поток {name} запущен.")
    parser = Parser()
    counter = 1
    while CATEGORY_QUEUE.qsize():
        game = CATEGORY_QUEUE.get()
        gamers_ids = parse_user_id_by_category(game.id, parser)
        logger.info(
            f"Поток {name}: Id игроков {game.name} {game.id} спаршены. Игроков:{len(gamers_ids)}. [{counter}]"
        )
        USERS_ID_DATA.extend(gamers_ids)
        counter += 1
    logger.info(f"Поток {name} завершён.")


def parse_gamers_multithread() -> None:
    """Запуск парсинга игроков в многопоточном режиме."""
    user_parsers = []
    for i in range(THREADS_NUM):
        name = f"Парсинг пользователей {i+1}"
        user_parsers.append(
            Thread(target=parse_gamers_thread,
                   kwargs={"name": name}, name=name)
        )
    [thread.start() for thread in user_parsers]
    [thread.join() for thread in user_parsers]


@logger.catch
def parse_gamers_thread(name: str) -> None:
    """Парсинг игроков."""
    logger.info(f"Поток {name} запущен.")
    parser = Parser()
    while MAIN_QUEUE.qsize():
        gamer_id = MAIN_QUEUE.get()
        logger.info(f"Поток {name}. Начал парсить пользователя {gamer_id}")
        try:
            res = Gamer(gamer_id, parser=parser).to_pandas_row()
            gamer_collection.insert_one(res)
            USER_DATA_QUEUE.put(res)
            logger.success(f"Поток {name}. Спарсил пользователя {gamer_id}")
        except:
            logger.error(f"Поток {name}. Ошибка пользователя {gamer_id}")
    logger.info(f"Поток {name} закончен.")


def make_parsing_queue() -> None:
    """Создание очереди для парсинга пользовательских данных."""
    global USERS_ID_DATA
    logger.info(f"Из категорий спаршено {len(USERS_ID_DATA)} id.")
    USERS_ID_DATA = set(USERS_ID_DATA)
    if CONTINUE_PARSING:
        USERS_ID_DATA -= set(load_today_data_from_db(gamer_collection).user_id)
    logger.info(f"Уникальных id для парсинга {len(USERS_ID_DATA)}.")

    USERS_ID_DATA = USERS_ID_DATA
    logger.info(
        f"Уникальных id для продолжения парсинга {len(USERS_ID_DATA)}.")

    for user_id in USERS_ID_DATA:
        MAIN_QUEUE.put(user_id)

    logger.info(f"Очередь создана. {MAIN_QUEUE.qsize()=}")


@logger.catch
def main() -> None:
    """Запуск парсера."""
    logger.info("Начало парсинга.")

    parse_id_multithread()
    make_parsing_queue()
    parse_gamers_multithread()
    write_gamers_data_to_file(gamer_collection)

    logger.info("Парсинг завершён.")

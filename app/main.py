from threading import Thread
from queue import Queue
from time import sleep
from typing import Any

from loguru import logger

from app.config import THREADS_NUM, FILES_PATH, FILE_ROTATION, CONTINUE_PARSING
from app.parse_types import Gamer
from app.parser import Parser
from app.pydantic_classes import Game
from app.utils import (
    func_chunks_generators,
    parse_categories_id,
    parse_user_id_by_category,
    write_gamers_data_to_file,
    write_ids_to_json,
    parse_used_id_from_xlsx
)


logger.add(
    "log/debug.log",
    format="{time} | {level} | {message}",
    level="DEBUG",
    rotation="12:00",
    compression="zip",
)


USERS_ID_DATA = []
CATEGORY_QUEUE: Queue[Game] = Queue()
MAIN_QUEUE: Queue[int] = Queue()
USER_DATA_QUEUE: Queue[dict[str, Any]] = Queue()


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


@logger.catch
def parse_user_data(name: str) -> None:
    """Парсинг игроков."""
    logger.info(f"Поток {name} запущен.")
    parser = Parser()
    while MAIN_QUEUE.qsize():
        USER_DATA_QUEUE.put(
            Gamer(MAIN_QUEUE.get(), parser=parser).to_pandas_row())
    logger.info(f"Поток {name} закончен.")


@logger.catch
def write_data_thread() -> None:
    """Поток записи в файлы."""
    name = 0
    logger.info("Поток записи в файлы запущен.")
    while True:
        if USER_DATA_QUEUE.qsize() > FILE_ROTATION:
            data: list[dict[str, Any]] = []
            for _ in range(FILE_ROTATION):
                data.append(USER_DATA_QUEUE.get())
            write_gamers_data_to_file(data, name)
            logger.info(f"Файл ~/data/{name}.xlsx записан.")
            name += 1
        else:
            sleep(300)


def parse_users_ids() -> None:
    games = parse_categories_id(parser=Parser())
    if isinstance(games, Exception):
        raise games
    
    for game in games:
        CATEGORY_QUEUE.put(game)
    category_threads = [
        Thread(
            target=parse_id_thread, kwargs={"name": f"Категории {i}"}, name=f"Категории {i}"
        )
        for i in range(1, THREADS_NUM+1)
    ]
    [thread.start() for thread in category_threads]
    [thread.join() for thread in category_threads]

    global USERS_ID_DATA

    write_ids_to_json(set(USERS_ID_DATA))

    if CONTINUE_PARSING:
        USERS_ID_DATA = list(set(CONTINUE_PARSING) / parse_used_id_from_xlsx)
    else:
        pass
    logger.info(f"Из категорий спаршено {len(USERS_ID_DATA)} id.")
    USERS_ID_DATA = set(USERS_ID_DATA)
    write_ids_to_json(USERS_ID_DATA)
    logger.info(f"Уникальных id для парсинга {len(USERS_ID_DATA)}.")

    for user_id in USERS_ID_DATA:
        MAIN_QUEUE.put(user_id)

    logger.info(f"Очередь создана. {MAIN_QUEUE.qsize()=}")


@logger.catch
def main():
    logger.info("Начало парсинга.")

    parse_users_ids()
    
    user_parsers = []
    for i in range(THREADS_NUM):
        name = f"Парсинг пользователей {i}"
        user_parsers.append(
            Thread(target=parse_user_data, kwargs={"name": name}, name=name))
    [thread.start() for thread in user_parsers]

    write_thread = Thread(target=write_data_thread, name="Поток записи")
    write_thread.start()

    [thread.join() for thread in user_parsers]
    write_thread.join()

    logger.info("Парсинг завершён.")

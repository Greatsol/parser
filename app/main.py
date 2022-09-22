from threading import Thread
from queue import Queue
from time import sleep
from typing import Any

from loguru import logger

from app.config import THREADS_NUM, FILES_PATH, FILE_ROTATION
from app.parse_types import Gamer
from app.parser import Parser
from app.pydantic_classes import Game
from app.utils import (
    func_chunks_generators,
    parse_categories_id,
    parse_user_id_by_category,
    write_gamers_data_to_file,
)


logger.add(
    "log/debug.log",
    format="{time} | {level} | {message}",
    level="DEBUG",
    rotation="12:00",
    compression="zip",
)


USERS_ID_DATA = []
MAIN_QUEUE: Queue[int] = Queue()
USER_DATA_QUEUE: Queue[dict[str, Any]] = Queue()


@logger.catch
def parse_id_thread(games: list[Game], name: str) -> None:
    """Поток парсинга id игороков из категорий."""
    logger.info(f"Поток {name} запущен.")
    parser = Parser()
    for counter, game in enumerate(games):
        gamers_ids = parse_user_id_by_category(game.id, parser)
        logger.info(
            f"Поток {name}: Id игроков {game.name} {game.id} спаршены. Игроков:{len(gamers_ids)}"
        )
        USERS_ID_DATA.extend(gamers_ids)


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


def main():
    logger.info("Начало парсинга.")
    games = parse_categories_id(parser=Parser())
    if isinstance(games, Exception):
        raise games
    chunks = func_chunks_generators(games, THREADS_NUM)
    category_threads = [
        Thread(
            target=parse_id_thread, args=(chunk, f"Категории {i}"), name=f"Категории {i}"
        )
        for i, chunk in enumerate(chunks)
    ]
    [thread.start() for thread in category_threads]
    [thread.join() for thread in category_threads]

    global USERS_ID_DATA
    logger.info(f"Из категорий спаршено {len(USERS_ID_DATA)} id.")
    USERS_ID_DATA = set(USERS_ID_DATA)
    logger.info(f"Уникальных id для парсинга {len(USERS_ID_DATA)}.")

    for user_id in USERS_ID_DATA:
        MAIN_QUEUE.put(user_id)

    logger.info(f"Очередь создана. {MAIN_QUEUE.qsize()=}")

    user_parsers = []
    for i in range(THREADS_NUM):
        name = f"Парсинг пользователей {i}"
        user_parsers.append(
            Thread(target=parse_user_data, args=(name), name=name))
    [thread.start() for thread in user_parsers]

    write_thread = Thread(target=write_data_thread, name="Поток записи")
    write_thread.start()

    [thread.join() for thread in user_parsers]
    write_thread.join()

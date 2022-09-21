from threading import Thread

from loguru import logger

from app.config import THREADS_NUM
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


@logger.catch
def parse_thread(games: list[Game], name: int) -> None:
    """Поток парсинга."""
    logger.info(f"Поток {name} запущен.")
    parser = Parser()
    for counter, game in enumerate(games):
        gamers_ids = parse_user_id_by_category(game.id, parser)
        logger.info(
            f"Поток {name}: Id игроков {game.name} {game.id} спаршены. Перехожу к парсингу подробной информации о них. Игроков:{len(gamers_ids)}"
        )
        gamers_data = tuple(
            Gamer(epal_id, parser).to_pandas_row() for epal_id in gamers_ids
        )
        write_gamers_data_to_file(gamers_data, game.name)
        logger.info(f"Файл {game.name}.xlsx записан.")
        logger.info(f"Поток {name}: {counter}/{len(games)} категорий.")
    logger.info(f"Поток {name} законченю")


def main():
    logger.info("Начало парсинга.")
    games = parse_categories_id(parser=Parser())
    if isinstance(games, Exception):
        raise games
    chunks = func_chunks_generators(games, THREADS_NUM)
    for i, chunk in enumerate(chunks):
        Thread(target=parse_thread, args=(chunk, i), name=str(i)).start()

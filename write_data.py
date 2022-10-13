from app.main import logger, gamer_collection, write_gamers_data_to_file


if __name__ == "__main__":
    write_gamers_data_to_file(gamer_collection)
    logger.success("Файл записан.")
import os

from dotenv import load_dotenv

load_dotenv()

TOR_PORT = os.environ["TOR_PORT"]
PROXY_LIST = os.environ["PROXY_LIST"]
THREADS_NUM = int(os.environ["THREADS_NUM"])
CONTINUE_PARSING = os.environ["CONTINUE_PARSING"] == "True"
DB_URI = os.environ["DB_URI"]
OUTPUT_EXCEL_PATH = os.environ["OUTPUT_EXCEL_PATH"]
LOG_ROTATION = os.environ["LOG_ROTATION"]

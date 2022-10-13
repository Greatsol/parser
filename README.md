# Epal parser | [@greatsol](http://t.me/greatsol)
## Окружение

Для работы парсера нужен tor, privoxy, mongodb, poetry, python3.10, pip3.
```sh
sudo apt update && sudo apt upgrade
```
```sh
sudo apt install software-properties-common -y
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.10
```
```sh
sudo apt install tor privoxy mongodb poetry python3-pip
pip3 install poetry
```

В [app/config.py](app/config.py) находится URI для бд. Можно заменить, а можно настроить монго по нему.

## Установка зависимостей
```sh
poetry install
```
## Настройка
Создать `.env` файл с настройкми бота (`.env_example -> .env`):
```bash
TOR_PORT=9050
PROXY_LIST=
THREADS_NUM=<THREADS_NUM>
CONTINUE_PARSING=False/True
DB_URI=<DB_URI>
OUTPUT_EXCEL_PATH=/root/data
LOG_ROTATION=<LOG_ROTATION>
```

## Запуск
```sh
poetry run python3 start_parser.py
```
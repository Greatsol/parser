from datetime import datetime
from typing import Any

from app.main import logger
from app.parser import Parser


class ValidationError(Exception):
    """Ошибка присваивания аттрибутов класса."""

    def __init__(self, field: str):
        self.message = f"Some {field} doesn't exist."
        super().__init__(self.message)


class Gamer:
    name: str
    userId: int
    raiting: float
    followers: int
    visitors: int
    bio: str
    serve_num: int
    timestamp: datetime

    def __init__(self, epal_id: int, parser: Parser) -> None:
        data = self.get_gamer_info(epal_id, parser=parser)
        if isinstance(data, Exception):
            self.make_empty_gamer(epal_id)
            return
        self.parse_fields(data, parser=parser, epal_id=epal_id)

    def make_empty_gamer(self, epal_id: int) -> None:
        self.user_id = epal_id
        self.name = "Invalid user"
        self.timestamp = datetime.now()

    def get_product_serve(self, epal_id: int, parser: Parser) -> int | Exception:
        data = {"productId": str(epal_id), "shareCode": ""}
        response = parser.request(
            "POST", path=f"https://play.epal.gg/web/product/detail", data=str(data)
        )
        if response == False:
            return 0
        response = response.json()
        if response["content"] is None:
            return Exception("No content in product response json.")
        return response["content"]["serveNum"]

    def get_gamer_info(
        self, epal_id: int, parser: Parser
    ) -> dict[str, str | int] | Exception:
        data = {"userId": epal_id}
        response = parser.request(
            "POST",
            path="https://play.epal.gg/web/user-search/info-to-detail",
            data=str(data),
        )
        if response == False:
            logger.error(f"{epal_id} недоступен.")
            return Exception
        response = response.json()
        if "content" not in response.keys():
            return Exception("Key content don't in product response json.")
        return response["content"]

    def to_pandas_row(self) -> dict[str, Any]:
        return self.__dict__

    def parse_fields(self, data: dict[str, Any], parser: Parser, epal_id: int) -> None:
        if not isinstance(data, dict):
            self.make_empty_gamer(epal_id)
            return
        self.name = data["nickname"]
        self.user_id = data["userId"]
        self.raiting = data["avgScore"]
        self.followers = data["followedCount"]
        self.visitors = data["visitedCount"]
        self.bio = data["userContent"]
        self.serve_num = data["serveNum"]
        for socialweb in data["socialList"]:
            setattr(self, socialweb["type"], socialweb["url"])
        for product in data["products"]:
            product_name = product["productName"]
            price = product["price"]
            unit = product["priceUnitDesc"]
            product_id = product["productId"]
            serve_num = self.get_product_serve(product_id, parser)
            setattr(self, product_name, f"{price}/{unit}")
            setattr(self, f"{product_name} count", serve_num)
        self.timestamp = datetime.now()
        self.date = str(datetime.now().date())


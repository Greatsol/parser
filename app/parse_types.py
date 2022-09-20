from datetime import datetime
from typing import Any

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
            raise Exception("Error self.get_gamer_info response.")
        self.parse_fields(data, parser=parser)

    def get_product_serve(self, epal_id: int, parser: Parser) -> int | Exception:
        data = {"productId": str(epal_id), "shareCode": ""}
        response = parser.request(
            "POST", path=f"https://play.epal.gg/web/product/detail", data=str(data))
        response = response.json()
        if response["content"] is None:
            return Exception("No content in product response json.")
        return response["content"]["serveNum"]

    def get_gamer_info(self, epal_id: int, parser: Parser) -> dict[str, str | int] | Exception:
        data = {"userId": epal_id}
        response = parser.request(
            "POST", path="https://play.epal.gg/web/user-search/info-to-detail", data=str(data))
        response = response.json()
        if "content" not in response.keys():
            return Exception("Key content don't in product response json.")
        return response["content"]

    def to_pandas_row(self) -> dict[str, Any]:
        return self.__dict__

    def parse_fields(self, data: dict[str, Any], parser: Parser) -> None:
        self.name = data["nickname"]
        self.user_id = data["userId"]
        self.raiting = data["avgScore"]
        self.followers = data["followedCount"]
        self.visitors = data["visitedCount"]
        self.bio = data["userContent"]
        self.serve_num = data["serveNum"]
        for socialweb in data["socialList"]:
            setattr(self, socialweb["type"], socialweb["url"])
        products = []
        for product in data["products"]:
            product_name = product["productName"]
            price = product["price"]
            unit = product["priceUnitDesc"]
            product_id = product["productId"]
            serve_num = self.get_product_serve(product_id, parser)
            products.append(
                f"{product_name} - {price}/{unit} [{serve_num}]")
        self.products = "\n".join(products)
        self.timestamp = datetime.now()

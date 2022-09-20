from pydantic import BaseModel, Field


class Game(BaseModel):
    """["global"]["gameMetas"]"""
    id: int
    name: str
    url: str = Field(alias="domain")


class Games(BaseModel):
    """["global"]"""
    games: list[Game] = Field(alias="gameMetas")

    def __len__(self) -> int:
        return len(self.games)

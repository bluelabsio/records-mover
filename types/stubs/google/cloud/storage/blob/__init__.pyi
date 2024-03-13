class Blob:
    name: str
    size: int

    def exists(self) -> bool:
        ...

    def delete(self) -> None:
        ...

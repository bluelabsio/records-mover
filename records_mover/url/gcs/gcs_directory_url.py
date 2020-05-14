from records_mover.url import BaseDirectoryUrl


class GCSDirectoryUrl(BaseDirectoryUrl):
    def __init__(self,
                 url: str,
                 **kwargs) -> None:
        self.url = url

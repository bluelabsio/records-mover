from typing import Optional, Iterable

# https://github.com/aws/aws-cli/blob/f1689c1034ba7e628ae82ee4e3d90061055923d3/awscli/clidriver.py


class CLIDriver:
    def main(self, args: Optional[Iterable[str]]) -> int:
        ...


def create_clidriver() -> CLIDriver:
    ...

import os
from awscli.clidriver import create_clidriver
from typing import Mapping


# https://github.com/boto/boto3/issues/358#issuecomment-372086466
def aws_cli(*cmd: str) -> None:
    old_env: Mapping[str, str] = dict(os.environ)
    try:
        # Environment
        env = os.environ.copy()
        env['LC_CTYPE'] = u'en_US.UTF'
        os.environ.update(env)

        # Run awscli in the same process
        exit_code = create_clidriver().main(args=cmd)

        # Deal with problems
        if exit_code > 0:
            raise RuntimeError('AWS CLI exited with code {}'.format(exit_code))
    finally:
        os.environ.clear()
        os.environ.update(old_env)

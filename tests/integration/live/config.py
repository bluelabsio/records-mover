"""Environment-driven configuration for the live acceptance suite.

Nothing here connects to anything; it only reads environment variables (and,
optionally, a dbt profile).  See README.md for the variables.
"""
import importlib
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import quote_plus

ENV_ENABLE = 'RECORDS_MOVER_LIVE'
_DBT_ENV_VAR = re.compile(r"\{\{\s*env_var\(\s*['\"]([^'\"]+)['\"]\s*"
                          r"(?:,\s*['\"]([^'\"]*)['\"])?\s*\)\s*\}\}")


class LiveConfigError(RuntimeError):
    pass


def is_enabled(env: Optional[Mapping[str, str]] = None) -> bool:
    return (os.environ if env is None else env).get(ENV_ENABLE) == '1'


def resolve_dbt_value(value: Any, env: Mapping[str, str]) -> Any:
    """Resolve dbt's ``{{ env_var('X') }}`` / ``{{ env_var('X', 'default') }}`` syntax."""
    if isinstance(value, str):
        m = _DBT_ENV_VAR.fullmatch(value.strip())
        if m:
            return env.get(m.group(1), m.group(2))
    return value


def read_dbt_output(profile: str, target: str, profiles_path: str,
                    env: Mapping[str, str]) -> Dict[str, Any]:
    yaml = importlib.import_module('yaml')  # untyped; imported only when a dbt profile is used
    try:
        with open(os.path.expanduser(profiles_path)) as f:
            profiles = yaml.safe_load(f)
        output = profiles[profile]['outputs'][target]
    except (OSError, KeyError, TypeError) as e:
        raise LiveConfigError(f"could not read dbt profile {profile!r} target {target!r} "
                              f"from {profiles_path}: {type(e).__name__}") from None
    return {k: resolve_dbt_value(v, env) for k, v in output.items()}


@dataclass(frozen=True)
class LiveConfig:
    # Contains the password: never include in repr/logs
    db_url: str = field(repr=False)
    schema: str
    allowed_schemas: Tuple[str, ...]
    s3_prefix: Optional[str]  # always ends with '/'
    aws_profile: str
    sslmode: str
    sslrootcert: Optional[str]  # None means certifi.where()
    statement_timeout_s: int


def _db_url(env: Mapping[str, str], problems: List[str]) -> Optional[str]:
    url = env.get('RECORDS_MOVER_LIVE_DB_URL')
    if url:
        return url
    profile = env.get('RECORDS_MOVER_LIVE_DBT_PROFILE')
    target = env.get('RECORDS_MOVER_LIVE_DBT_TARGET')
    if not (profile and target):
        problems.append('set RECORDS_MOVER_LIVE_DB_URL, or both RECORDS_MOVER_LIVE_DBT_PROFILE '
                        'and RECORDS_MOVER_LIVE_DBT_TARGET')
        return None
    cfg = read_dbt_output(profile, target,
                          env.get('RECORDS_MOVER_LIVE_DBT_PROFILES', '~/.dbt/profiles.yml'), env)
    user = cfg.get('user')
    password = cfg.get('password', cfg.get('pass'))
    dbname = cfg.get('dbname', cfg.get('database'))
    if not (user and password and cfg.get('host') and dbname):
        problems.append('dbt profile needs user, password, host and dbname')
        return None
    return (f"redshift+psycopg2://{quote_plus(str(user))}:{quote_plus(str(password))}"
            f"@{cfg['host']}:{cfg.get('port', 5439)}/{dbname}")


def load_config(env: Optional[Mapping[str, str]] = None) -> LiveConfig:
    env = os.environ if env is None else env
    problems: List[str] = []
    db_url = _db_url(env, problems)

    schema = env.get('RECORDS_MOVER_LIVE_SCHEMA', '').strip()
    allowed = tuple(s.strip() for s in
                    env.get('RECORDS_MOVER_LIVE_ALLOWED_SCHEMAS', '').split(',') if s.strip())
    if not schema:
        problems.append('RECORDS_MOVER_LIVE_SCHEMA is required')
    if not allowed:
        problems.append('RECORDS_MOVER_LIVE_ALLOWED_SCHEMAS is required (comma-separated)')
    elif schema and schema.lower() not in [a.lower() for a in allowed]:
        problems.append(f"schema {schema!r} is not in RECORDS_MOVER_LIVE_ALLOWED_SCHEMAS")

    s3_prefix = env.get('RECORDS_MOVER_LIVE_S3_PREFIX', '').strip() or None
    if s3_prefix:
        if not s3_prefix.startswith('s3://') or len(s3_prefix) <= len('s3://'):
            problems.append('RECORDS_MOVER_LIVE_S3_PREFIX must look like s3://bucket/prefix/')
        elif not s3_prefix.endswith('/'):
            s3_prefix += '/'

    try:
        timeout = int(env.get('RECORDS_MOVER_LIVE_STATEMENT_TIMEOUT_S', '120'))
    except ValueError:
        problems.append('RECORDS_MOVER_LIVE_STATEMENT_TIMEOUT_S must be an integer')
        timeout = 120

    if problems or db_url is None:
        raise LiveConfigError('live suite misconfigured: ' + '; '.join(problems))
    return LiveConfig(db_url=db_url,
                      schema=schema,
                      allowed_schemas=allowed,
                      s3_prefix=s3_prefix,
                      aws_profile=env.get('RECORDS_MOVER_LIVE_AWS_PROFILE', 'default'),
                      sslmode=env.get('RECORDS_MOVER_LIVE_SSLMODE', 'verify-full'),
                      sslrootcert=env.get('RECORDS_MOVER_LIVE_SSLROOTCERT') or None,
                      statement_timeout_s=timeout)

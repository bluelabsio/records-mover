from subprocess import check_output
from records_mover.logging import register_secret
from db_facts.db_facts_types import DBFacts


def lpass_field(name: str, field: str) -> str:
    if field == 'notes':
        field_arg = '--notes'
    elif field == 'username':
        field_arg = '--username'
    elif field == 'password':
        field_arg = '--password'
    elif field == 'url':
        field_arg = '--url'
    else:
        field_arg = '--field=' + field
    raw_output = check_output(['lpass',
                               'show',
                               field_arg,
                               name])
    return raw_output.decode('utf-8').rstrip('\n')


def db_facts_from_lpass(lpass_entry_name: str) -> DBFacts:
    from records_mover.db.db_type import canonicalize_db_type, db_protocol

    user = lpass_field(lpass_entry_name, 'username')
    password = lpass_field(lpass_entry_name, 'password')
    register_secret(password)
    host = lpass_field(lpass_entry_name, 'Hostname')
    port = int(lpass_field(lpass_entry_name, 'Port'))
    raw_db_type = lpass_field(lpass_entry_name, 'Type')
    db_type = canonicalize_db_type(raw_db_type)
    dbname = lpass_field(lpass_entry_name, 'Database')

    return {'password': password,
            'host': host,
            'user': user,
            'type': db_type,
            'protocol': db_protocol(db_type),
            'port': port,
            'database': dbname}

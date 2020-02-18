from subprocess import check_output
from records_mover.db.db_type import canonicalize_db_type, db_protocol
from ..utils import deprecated
from db_facts.db_facts_types import DBFacts


# This function was written under Python 2.7 and never got fixed to be
# Python 3 compatible, so it kept returning a byte array when it
# should have been returning a string.  It also never chomped the last
# newline off of the command line output, so the API to it was never
# great.  Meanwhile, clients of it started to use it and then expect
# the results to be byte arrays, so just changing behavior isn't a
# great option regardless.
#
# So, let's deprecate it until the next major version here.
#
# Please switch code over to use lpass_field instead.
@deprecated
def lpass_show(name: str, field: str) -> bytes:
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
    return check_output(['lpass',
                         'show',
                         field_arg,
                         name])


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
    user = lpass_field(lpass_entry_name, 'username')
    password = lpass_field(lpass_entry_name, 'password')
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

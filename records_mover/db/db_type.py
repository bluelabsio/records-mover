

def canonicalize_db_type(db_type: str) -> str:
    canonical_db_type = {
        'vsql': 'vertica',
        'postgresql': 'postgres',
        'psql (redshift)': 'redshift',
        'psql': 'postgres',
        'Redshift': 'redshift'
    }
    lowercased_type = db_type.lower()
    return canonical_db_type.get(lowercased_type,
                                 lowercased_type)


def db_protocol(db_type: str) -> str:
    final_protocol = {
        'redshift': 'postgres',
    }
    return final_protocol.get(db_type, db_type)

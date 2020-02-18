from mock import Mock
from io import BytesIO
import json


def fake_s3_object(bucket, key):
    s3_result = Mock()

    def generate_s3_response(body):
        body_bytes = body.encode('utf-8')
        s3_result.get.return_value = {
            'Body': BytesIO(body_bytes)
        }

    if (bucket == 'mybucket' and key == 'myparent/mychild/_format_delimited'):
        format_data = {
            'type': 'delimited',
            'variant': 'bluelabs',
            'hints': {
                'field-delimiter': ',',
                'record-terminator': '\n',
                'compression': 'GZIP',
                'quoting': None,
                'quotechar': '"',
                'doublequote': False,
                'escape': '\\',
                'encoding': 'UTF8',
                'dateformat': 'YYYY-MM-DD',
                'timeonlyformat': 'HH12:MI AM',
                'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
                'datetimeformat': 'YYYY-MM-DD HH24:MI:SS'
            }
        }
        body = json.dumps(format_data)
        generate_s3_response(body)
        return s3_result
    elif (bucket == 'mybucket' and key == 'myparent/mychild/_schema'):
        generate_s3_response('CREATE TABLE somethingorother;')
        return s3_result
    else:
        raise NotImplementedError(f"Don't know about bucket={bucket} and key={key}")

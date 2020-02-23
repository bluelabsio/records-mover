from records_mover.airflow.airflow_job_context import AirflowJobContext
import unittest
from odictliteral import odict
from mock import patch

test_config_schema = {
    "type": "object",
    "properties": odict[
        'sourceType': {
            'enum': ['url_file', 'github_file']
        },
        'url': {
            'type': 'string'
        },
        'fire_phasers': {
            'type': 'boolean',
        },
        'phaserIntensity': {
            'type': 'integer',
            'default': 50
        },
        'a': {
            'type': 'object',
            'properties': odict[
                'b': {
                    'type': 'string',
                }
            ]
        },
        'outputType': {
            'enum': ['sql'],
            'default': 'sql',
        }
    ],
    "required": ["sourceType"]
}

config = {
    'url': 'http://abc',
    'fire_phasers': True,
    'a': {
        'b': 'c'
    },
    'sourceType': 'url_file'
}


class TestAirflowJobContext(unittest.TestCase):
    @patch('records_mover.airflow.airflow_job_context.CredsViaAirflow')
    def test_creds(self, mock_CredsViaAirflow):
        context = AirflowJobContext(default_db_creds_name=None,
                                    default_aws_creds_name=None,
                                    config_json_schema=test_config_schema)
        self.assertEqual(mock_CredsViaAirflow.return_value, context.creds)

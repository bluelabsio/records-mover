import unittest
from odictliteral import odict
from records_mover.cli.job_config_schema_as_args_parser import JobConfigSchemaAsArgsParser


class TestJobConfigSchemaAsArgsParser(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.job_config_schema = {
            "type": "object",
            "properties": odict[
                'divisions': {
                    'description': 'How to divide data',
                    'type': 'array',
                    'items': {
                        'type': 'string',
                    },
                    'default': ['candidates', 'counties', 'townships', 'precincts']
                },
                'fooint': {
                    'type': 'integer',
                },
                'barofint': {
                    'type': 'array',
                    'items': {
                        'type': 'integer',
                    },
                },
                'boolwithdefaulttrue': {
                    'type': 'boolean',
                    'default': True,
                },
                'boolwithdefaultfalse': {
                    'type': 'boolean',
                    'default': False,
                },
                'enumwithnulloption': {
                    'type': ['string', 'null'],
                    'enum': ['a', 'b', None]
                },
                'nested_dict': {
                    'type': 'object',
                    'properties': {
                        'object_key': {
                            'type': 'string'
                        }
                    },
                },
            ],
            'required': ['nested_dict'],
        }

    def test_dict_of_dict(self):
        parser = JobConfigSchemaAsArgsParser.from_description(self.job_config_schema, 'testjob')
        out = parser.parse(['--nested_dict.object_key', 'a'])
        expected = {
            'nested_dict': {
                'object_key': 'a'
            },
            'divisions': ['candidates', 'counties', 'townships', 'precincts'],
            'boolwithdefaultfalse': False, 'boolwithdefaulttrue': True
        }
        self.assertEqual(expected, out)

    def test_array_of_strings_overridden(self):
        parser = JobConfigSchemaAsArgsParser.from_description(self.job_config_schema, 'testjob')
        out = parser.parse(['--divisions', 'a'])
        expected = {
            'divisions': ['a'], 'boolwithdefaultfalse': False, 'boolwithdefaulttrue': True
        }
        self.assertEqual(expected, out)

    def test_array_of_strings_default(self) -> None:
        parser = JobConfigSchemaAsArgsParser.from_description(self.job_config_schema, 'testjob')
        out = parser.parse([])
        expected = {
            'divisions': ['candidates', 'counties', 'townships', 'precincts'],
            'boolwithdefaultfalse': False, 'boolwithdefaulttrue': True
        }
        self.assertEqual(expected, out)

    def test_enum_can_be_none(self) -> None:
        parser = JobConfigSchemaAsArgsParser.from_description(self.job_config_schema, 'testjob')
        out = parser.parse(['--no_enumwithnulloption'])
        expected = {
            'divisions': ['candidates', 'counties', 'townships', 'precincts'],
            'boolwithdefaultfalse': False, 'boolwithdefaulttrue': True,
            'enumwithnulloption': None
        }
        self.assertEqual(expected, out)

    def test_enum_can_be_set(self) -> None:
        parser = JobConfigSchemaAsArgsParser.from_description(self.job_config_schema, 'testjob')
        out = parser.parse(['--enumwithnulloption', 'a'])
        expected = {
            'divisions': ['candidates', 'counties', 'townships', 'precincts'],
            'boolwithdefaultfalse': False, 'boolwithdefaulttrue': True,
            'enumwithnulloption': 'a'
        }
        self.assertEqual(expected, out)

    def test_enum_cannot_be_invalid(self) -> None:
        parser = JobConfigSchemaAsArgsParser.from_description(self.job_config_schema, 'testjob')
        with self.assertRaises(SystemExit) as r:
            parser.parse(['--enumwithnulloption', 'x'])

        self.assertEqual(str(r.exception.__context__),
                         "argument --enumwithnulloption: invalid choice: 'x' "
                         "(choose from 'a', 'b')")

    def test_bad_syntax_1(self):
        bad_job_config_schema = {
            "type": "object",
            "properties": odict[
                'divisions': 123  # should be another json schema, not a number...
            ],
        }
        parser = JobConfigSchemaAsArgsParser.from_description(bad_job_config_schema, 'testjob')
        with self.assertRaises(TypeError) as r:
            parser.parse([])
        self.assertEqual(str(r.exception),
                         'Did not understand [123] in [odict[divisions: 123]]')

    def test_bad_syntax_2(self):
        bad_job_config_schema = {
            "type": "object",
            "properties": odict[
                'divisions': {
                    'type': 'array',
                    'items': 123,
                },
            ],
        }
        parser = JobConfigSchemaAsArgsParser.from_description(bad_job_config_schema, 'testjob')
        with self.assertRaises(TypeError) as r:
            parser.parse([])
        self.assertEqual(str(r.exception),
                         "Did not understand [123] in "
                         "[odict[divisions: {'type': 'array', 'items': 123}]]")

    def test_bad_syntax_3(self):
        bad_job_config_schema = {
            "type": "object",
            "properties": odict[
                'divisions': {
                    'type': 'array',
                    'items': {
                        'type': 123
                    },
                },
            ],
        }
        parser = JobConfigSchemaAsArgsParser.from_description(bad_job_config_schema, 'testjob')
        with self.assertRaises(TypeError) as r:
            parser.parse([])
        self.assertEqual(str(r.exception),
                         "Did not understand [123] in "
                         "[odict[divisions: {'type': 'array', 'items': {'type': 123}}]]")

    def test_bad_syntax_4(self):
        bad_job_config_schema = {
            "type": "object",
            "properties": 123
        }
        parser = JobConfigSchemaAsArgsParser.from_description(bad_job_config_schema, 'testjob')
        with self.assertRaises(TypeError) as r:
            parser.parse([])
        self.assertEqual(str(r.exception),
                         "Did not understand 123 in "
                         "{'type': 'object', 'properties': 123}")

    def test_bad_syntax_5(self):
        bad_job_config_schema = {
            "type": "object",
            "properties": {
                'fooobject': {
                    'type': 'object',
                    'properties': 123
                },
            },
        }
        parser = JobConfigSchemaAsArgsParser.from_description(bad_job_config_schema, 'testjob')
        with self.assertRaises(TypeError) as r:
            parser.parse([])
        self.assertEqual(str(r.exception),
                         "Did not understand [123] in "
                         "[{'fooobject': {'type': 'object', 'properties': 123}}]")

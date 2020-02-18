import unittest
from unittest.mock import patch
from records_mover.creds import lpass


class TestLPass(unittest.TestCase):
    @patch('records_mover.creds.lpass.check_output')
    def test_lpass_show_notes(self, mock_check_output):
        lpass.lpass_show('my_name', 'notes')
        mock_check_output.\
            assert_called_with(['lpass', 'show', '--notes', 'my_name'])

    @patch('records_mover.creds.lpass.check_output')
    def test_lpass_show_username(self, mock_check_output):
        lpass.lpass_show('my_name', 'username')
        mock_check_output.\
            assert_called_with(['lpass', 'show', '--username', 'my_name'])

    @patch('records_mover.creds.lpass.check_output')
    def test_lpass_show_password(self, mock_check_output):
        lpass.lpass_show('my_name', 'password')
        mock_check_output.\
            assert_called_with(['lpass', 'show', '--password', 'my_name'])

    @patch('records_mover.creds.lpass.check_output')
    def test_lpass_show_url(self, mock_check_output):
        lpass.lpass_show('my_name', 'url')
        mock_check_output.\
            assert_called_with(['lpass', 'show', '--url', 'my_name'])

    @patch('records_mover.creds.lpass.check_output')
    def test_lpass_show_field1(self, mock_check_output):
        lpass.lpass_show('my_name', 'field1')
        mock_check_output.\
            assert_called_with(['lpass', 'show', '--field=field1', 'my_name'])

    @patch('records_mover.creds.lpass.check_output')
    def test_lpass_field_notes(self, mock_check_output):
        mock_check_output.return_value = "fakenotes\n".encode("utf-8")
        out = lpass.lpass_field('my_name', 'notes')
        mock_check_output.\
            assert_called_with(['lpass', 'show', '--notes', 'my_name'])
        assert out == "fakenotes"

    @patch('records_mover.creds.lpass.check_output')
    def test_lpass_field_username(self, mock_check_output):
        mock_check_output.return_value = "fakeuser\n".encode("utf-8")
        out = lpass.lpass_field('my_name', 'username')
        mock_check_output.\
            assert_called_with(['lpass', 'show', '--username', 'my_name'])
        assert out == "fakeuser"

    @patch('records_mover.creds.lpass.check_output')
    def test_lpass_field_password(self, mock_check_output):
        mock_check_output.return_value = "fakepassword\n".encode("utf-8")
        out = lpass.lpass_field('my_name', 'password')
        mock_check_output.\
            assert_called_with(['lpass', 'show', '--password', 'my_name'])
        assert out == "fakepassword"

    @patch('records_mover.creds.lpass.check_output')
    def test_lpass_field_url(self, mock_check_output):
        mock_check_output.return_value = "fakeurl\n".encode("utf-8")
        out = lpass.lpass_field('my_name', 'url')
        mock_check_output.\
            assert_called_with(['lpass', 'show', '--url', 'my_name'])
        assert out == "fakeurl"

    @patch('records_mover.creds.lpass.check_output')
    def test_lpass_field_field1(self, mock_check_output):
        mock_check_output.return_value = "fakefield1\n".encode("utf-8")
        out = lpass.lpass_field('my_name', 'field1')
        mock_check_output.\
            assert_called_with(['lpass', 'show', '--field=field1', 'my_name'])
        assert out == "fakefield1"

    @patch('records_mover.creds.lpass.check_output')
    def test_db_facts_from_lpass(self, mock_check_output):
        def fake_check_output(args):
            assert args[0] == 'lpass'
            assert args[1] == 'show'
            assert args[3] == 'my_lpass_name'
            ret = {
                "--username": 'fakeuser',
                "--password": 'fakepassword',
                "--field=Hostname": 'fakehost',
                "--field=Port": '123',
                "--field=Type": 'faketype',
                "--field=Database": 'fakedatabase',
            }
            return (ret[args[2]] + "\n").encode('utf-8')
        mock_check_output.side_effect = fake_check_output
        db_facts = lpass.db_facts_from_lpass('my_lpass_name')
        expected_db_facts = {
            'database': 'fakedatabase',
            'host': 'fakehost',
            'password': 'fakepassword',
            'port': 123,
            'type': 'faketype',
            'user': 'fakeuser',
            'protocol': 'faketype',  # if we don't know, just pass through
        }
        assert db_facts == expected_db_facts

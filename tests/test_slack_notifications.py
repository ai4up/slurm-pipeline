from unittest.mock import patch

from slack.errors import SlackApiError

import slurm_pipeline.slack_notifications as sn
from slurm_pipeline.slack_notifications import SlackLoggingHandler


@patch.object(sn.WebClient, 'chat_postMessage', return_value={'ts': 'new-thread-id', 'channel': 'some-channel'})
def test_send_message(mock):
    thread_id, channel = sn.send_message('some-message', 'some-channel', 'some-token')

    a = mock.call_args.kwargs
    assert a['text'] == 'some-message'
    assert a['channel'] == 'some-channel'
    assert a['thread_ts'] == None
    assert thread_id == 'new-thread-id'
    assert channel == 'some-channel'


@patch.object(sn.WebClient, 'chat_postMessage', return_value={'ts': 'new-thread-id', 'channel': 'some-channel'})
def test_send_code_block_message(mock):
    long_code_block_message = (
        "Some text before the code block.\n" +
        "```\n" +
        "This is a sample code block.\n" * 500 +  # Simulates a long code block
        "```\n" +
        "Some text after the code block."
    )

    sn.send_message(long_code_block_message, 'some-channel', 'some-token')

    a = mock.call_args.kwargs
    assert a['text'].count('```') == 2
    assert a['text'][:3] == '```'
    assert a['text'][-31:] == 'Some text after the code block.'
    assert mock.call_count == 4


@patch.object(sn.WebClient, 'chat_postMessage', side_effect=SlackApiError('msg', 'res'))
def test_send_message_exception(mock):
    SlackLoggingHandler.add_to_logger(sn.logger, '#some-channel', 'some-token')

    sn.send_message('some-message', 'channel', 'token')

    assert mock.call_count == 1

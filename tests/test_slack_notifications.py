from unittest.mock import patch

from slack.errors import SlackApiError

import slurm_pipeline.slack_notifications as sn
from slurm_pipeline.slack_notifications import SlackHandler


@patch.object(sn.WebClient, 'chat_postMessage', return_value={'ts': 'new-thread-id'})
def test_send_message(mock):
    thread_id = sn.send_message('some-message', 'some-channel', 'some-token')

    a = mock.call_args.kwargs
    assert a['text'] == 'some-message'
    assert a['channel'] == 'some-channel'
    assert a['thread_ts'] == None
    assert thread_id == 'new-thread-id'


@patch.object(sn.WebClient, 'chat_postMessage', side_effect=SlackApiError('msg', 'res'))
def test_send_message_exception(mock):
    SlackHandler.add_to_logger(sn.logger, '#some-channel', 'some-token')

    sn.send_message('some-message', 'channel', 'token')

    assert mock.call_count == 1

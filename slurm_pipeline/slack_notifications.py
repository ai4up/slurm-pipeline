import logging
from logging import StreamHandler

from slack import WebClient
from slack.errors import SlackApiError

logger = logging.getLogger(__name__)


class SlackHandler(StreamHandler):

    def __init__(self, slack_channel, slack_token, slack_thread_id=None):
        StreamHandler.__init__(self)
        self.slack_channel = slack_channel
        self.slack_token = slack_token
        self.slack_thread_id = slack_thread_id


    def emit(self, record):
        msg = self.format(record)
        send_message(msg, self.slack_channel, self.slack_token, self.slack_thread_id)


def send_message(message, channel, token, thread_id=None):
    try:
        client = WebClient(token)
        response = client.chat_postMessage(
            channel=channel,
            text=message,
            thread_ts=thread_id
        )
        return response['ts']

    except SlackApiError as e:
        logger.error(f'Error occurred sending slack message to {channel}: {e}')

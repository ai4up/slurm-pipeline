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


    @staticmethod
    def add_to_logger(logger, channel, token, thread_id=None, log_lvl=logging.CRITICAL):
        sh = SlackHandler(channel, token, thread_id)
        sh.setLevel(log_lvl)
        logger.addHandler(sh)


    def emit(self, record):
        msg = self.format(record)
        try:
            send_message(msg, self.slack_channel, self.slack_token, self.slack_thread_id)
        except Exception as e:
            _handle_exception(f'{msg} - {e}')


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
        _handle_exception(f'Error occurred sending slack message to channel {channel}: {e}')


def react(emoji, thread_id, channel, token):
    try:
        client = WebClient(token)
        response = client.reactions_add(
            channel=channel,
            name=emoji,
            timestamp=thread_id
        )
        return response['ts']

    except SlackApiError as e:
        _handle_exception(f'Error occurred reacting to slack message in channcel {channel}: {e}')


def _handle_exception(msg):
    old_handlers = logger.handlers[:]
    logger.handlers = [h for h in logger.handlers if not isinstance(h, SlackHandler)]
    logger.error(msg)
    logger.handlers = old_handlers

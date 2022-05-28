import logging

from slack import WebClient
from slack.errors import SlackApiError

logger = logging.getLogger(__name__)


def send_message(message, channel, token):
    try:
        client = WebClient(token)
        response = client.chat_postMessage(
            channel=channel,
            text=message
        )
        return response['ok']

    except SlackApiError as e:
        logger.error(f'Error occurred sending slack message to {channel}: {e}')
        return e.response['ok']

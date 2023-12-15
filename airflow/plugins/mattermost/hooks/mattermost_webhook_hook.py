import json

from airflow import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class MattermostWebhookHook(HttpHook):

    """
    Send messages to Mattermost using webhooks. For more details on the parameters, please check
    `Incoming webhooks <https://developers.mattermost.com/integrate/webhooks/incoming/#parameters>_` documentation.

    :param mattermost_conn_id: :ref:`http connection<howto/connection:http>` that has the base
        url i.e https://mattermost.example.com/ and webhook's id under password.
    :param text: The message to send.
    :param channel:	Overrides the channel the message posts in. Use the channel’s name and not the display name, e.g.
        use town-square, not Town Square.
    :param username: Overrides the username the message posts as.
    :param type: Sets the post type, mainly for use by plugins.
    :param props: Sets the post props, a JSON property bag for storing extra or meta data on the post.
    """

    conn_name_attr = 'mattermost_conn_id'
    default_conn_name = 'mattermost_default'
    conn_type = 'mattermostwebhook'
    hook_name = 'Mattermost Webhook'

    def __init__(
        self,
        *,
        mattermost_conn_id=None,
        text=None,
        channel=None,
        username=None,
        icon_url=None,
        icon_emoji=None,
        attachments=None,
        type=None,
        props=None,
        **kwargs,
    ):
        super().__init__(http_conn_id=mattermost_conn_id, **kwargs)
        self.text = text
        self.channel = channel
        self.username = username
        self.icon_url = icon_url
        self.icon_emoji = icon_emoji
        self.attachments = attachments
        self.type = type
        self.props = props

    def _build_endpoint(self) -> str:
        if not self.http_conn_id:
            raise AirflowException('Failed to create Mattermost client. No http_conn_id provided')
        conn = self.get_connection(self.http_conn_id)
        if getattr(conn, 'password', None):
            return f'/hooks/{conn.password}'
        raise AirflowException('Failed to create Mattermost client. No token provided')

    def _build_message(self) -> str:
        msg = {
            'text': self.text,
            'channel': self.channel,
            'username': self.username,
            'type': self.type,
            'props': self.props,
            'icon_url': self.icon_url,
            'icon_emoji': self.icon_emoji,
            'attachments': self.attachments,
        }
        return json.dumps({k: v for k, v in msg.items() if v is not None})

    def execute(self) -> None:
        msg = self._build_message()
        self.run(
            endpoint=self._build_endpoint(),
            data=msg,
            headers={'Content-type': 'application/json'},
            extra_options={'check_response': True},
        )

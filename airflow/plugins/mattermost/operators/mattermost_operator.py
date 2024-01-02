from mattermost.hooks.mattermost_webhook_hook import MattermostWebhookHook

from airflow.providers.http.operators.http import SimpleHttpOperator


class MattermostOperator(SimpleHttpOperator):

    """
    Operator that allows sending messages to Mattermost. For more details on the parameters, please check
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

    template_fields = ('text', 'channel', 'username', 'type', 'props', 'attachments')

    def __init__(
        self,
        *,
        mattermost_conn_id,
        text=None,
        channel=None,
        username=None,
        icon_url=None,
        icon_emoji=None,
        type=None,
        props=None,
        attachments=None,
        **kwargs,
    ) -> None:
        super().__init__(endpoint=None, **kwargs)
        self.mattermost_conn_id = mattermost_conn_id
        self.text = text
        self.channel = channel
        self.username = username
        self.icon_url = icon_url
        self.icon_emoji = icon_emoji
        self.type = type
        self.props = props
        self.attachments = attachments

    def hook(self):
        return MattermostWebhookHook(
            mattermost_conn_id=self.mattermost_conn_id,
            text=self.text,
            channel=self.channel,
            username=self.username,
            icon_url=self.icon_url,
            icon_emoji=self.icon_emoji,
            type=self.type,
            props=self.props,
            attachments=self.attachments,
        )

    def execute(self, context):
        self.hook().execute()

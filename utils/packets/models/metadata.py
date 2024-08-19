from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class SupportPacketTypeEnum(str, Enum):
    # See https://github.com/mattermost/mattermost/blob/bcf92d3f53dc396a9a0350be4347b7c71f77f68e/server/public/model/packet_metadata.go#L17   # noqa: E501
    # for more details.
    support_packet = 'support-packet'
    plugin_packet = 'plugin-packet'


class Extras(BaseModel, extra='allow'):
    plugin_id: str
    plugin_version: str


class SupportPacketMetadata(BaseModel, extra='ignore'):
    version: int = Field(ge=1)
    type: SupportPacketTypeEnum
    generated_at: datetime
    server_version: str
    server_id: str = Field(min_length=26, max_length=26)
    license_id: str | None = Field(None, min_length=0, max_length=26)
    customer_id: str | None = Field(None, min_length=0, max_length=26)
    extras: Extras | None = Field(None)

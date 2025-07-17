import typing as t

from pydantic import BaseModel, ConfigDict, Field
from pydantic.fields import FieldInfo
from redis.asyncio.retry import Retry


class Doc(FieldInfo):
    pass


CanBeImported = t.Annotated[
    str,
    Doc(
        description="can be imported by unfazed.utils.import_string",
        example="unfazed.core.Unfazed",
    ),
]


class RedisOptions(BaseModel):
    prefix: str | None = Field(
        None,
        alias="PREFIX",
        description="its strongly recommended to set prefix",
    )
    version: str | None = Field(None, alias="VERSION", description="version of cache")
    retry: t.Optional[Retry] = None

    socket_timeout: int | None = None
    socket_connect_timeout: int | None = None
    socket_keepalive: bool | None = None
    socket_keepalive_options: t.Mapping[int, t.Union[int, bytes]] | None = None

    # set decode responses to True
    decode_responses: bool = False
    retry_on_timeout: bool = False
    retry_on_error: t.List | None = None
    max_connections: int = 10
    single_connection_client: bool = False
    health_check_interval: int = 30

    ssl: bool = False
    ssl_keyfile: str | None = None
    ssl_certfile: str | None = None
    ssl_cert_reqs: str = "required"
    ssl_ca_certs: str | None = None
    ssl_ca_data: str | None = None
    ssl_check_hostname: bool = False
    ssl_min_version: t.Any = None
    ssl_ciphers: str | None = None

    serializer: CanBeImported | None = Field(
        "unfazed.cache.serializers.pickle.PickleSerializer",
        alias="SERIALIZER",
        description="serialize data before save",
    )

    compressor: CanBeImported | None = Field(
        "unfazed.cache.compressors.zlib.ZlibCompressor",
        alias="COMPRESSOR",
        description="compress data before save",
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

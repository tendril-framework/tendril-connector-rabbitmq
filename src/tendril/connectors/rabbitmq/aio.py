

from functools import partial
from pika import ConnectionParameters
from contextlib import asynccontextmanager
from aio_pika import Message
from aio_pika import connect_robust
from aio_pika import DeliveryMode
from aio_pika import ExchangeType
from aio_pika.pool import Pool

from tendril.core.mq.aio_base import GenericMQAsyncClient
from tendril.core.mq.aio_base import GenericMQAsyncManager
from tendril.core.mq.aio_base import MQServerNotRecognized
from tendril.core.mq.aio_base import MQServerNotEnabled

from tendril.config import MQ_SERVER_CODES
from tendril.config import MQ_SERVER_SSL
from tendril import config

from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


if MQ_SERVER_SSL:
    scheme = 'amqps'
else:
    scheme = 'amqp'

def _build_conn_string(p: ConnectionParameters):
    return f"{scheme}://{p.credentials.username}:{p.credentials.password}" \
           f"@{p.host}:{p.port}/{p.virtual_host}"


def _pp(code):
    if code == 'default':
        return ''
    else:
        return code


class RabbitMQAsyncManager(GenericMQAsyncManager):
    _instance = None

    @classmethod
    async def get_instance(cls):
        if cls._instance is None:
            cls._instance = RabbitMQAsyncManager()
        return cls._instance

    async def init_connection(self, loop, code):
        # Use the connect function to create a connection object with the default parameters
        # The connection object will be reused until it is closed explicitly
        mq_server_parameters = getattr(config, 'MQ{}_SERVER_PARAMETERS'.format(code))
        connection_string = _build_conn_string(mq_server_parameters)
        logger.info(f"Connecting to RabbitMQ at {mq_server_parameters.host}")
        connection = await connect_robust(url=connection_string, loop=loop,
                                          client_properties={'name': log.identifier})
        return connection

    async def init_exchange(self, code):
        async with self.channel_pools[code or 'default'].acquire() as channel:
            mq_server_parameters = getattr(config, 'MQ{}_SERVER_PARAMETERS'.format(code))
            mq_server_exchange = getattr(config, 'MQ{}_SERVER_EXCHANGE'.format(code))
            logger.debug(f"Creating the {mq_server_exchange} exchange on "
                         f"RabbitMQ at {mq_server_parameters.host}")
            _exchange = await channel.declare_exchange(
                mq_server_exchange, ExchangeType.TOPIC, durable=True
            )

    async def init(self, loop):
        self.connections = {}
        self.channel_pools = {}

        async def get_channel(code):
            mq_server_parameters = getattr(config, 'MQ{}_SERVER_PARAMETERS'.format(code))
            logger.debug(f"Creating a channel to RabbitMQ at {mq_server_parameters.host}")
            channel = await self.connections[code or 'default'].channel()
            await channel.set_qos(prefetch_count=1)
            return channel

        for code in MQ_SERVER_CODES:
            enabled = getattr(config, 'MQ{}_ENABLED'.format(code))
            if not enabled:
                continue
            self.connections[code or 'default'] = await self.init_connection(loop, code)
            self.channel_pools[code or 'default']: Pool = Pool(partial(get_channel, code), max_size=10, loop=loop)
            await self.init_exchange(code)

    async def close(self):
        for code in MQ_SERVER_CODES:
            enabled = getattr(config, 'MQ{}_ENABLED'.format(code))
            if not enabled:
                continue
            mq_server_parameters = getattr(config, 'MQ{}_SERVER_PARAMETERS'.format(code))
            logger.info(f"Closing the channel pool to RabbitMQ at {mq_server_parameters.host}")
            await self.channel_pools[code or 'default'].close()
            self.channel_pools[code or 'default'] = None
            logger.info(f"Disconnecting from RabbitMQ at {mq_server_parameters.host}")
            await self.connections[code or 'default'].close()
            self.connections[code or 'default'] = None

    @asynccontextmanager
    async def get_channel(self, code='default'):
        if code not in MQ_SERVER_CODES and code != 'default':
            raise MQServerNotRecognized(code=code)
        if code not in self.channel_pools.keys():
            raise MQServerNotEnabled(code=code)
        async with self.channel_pools[code].acquire() as channel:
            yield channel


class RabbitMQAsyncClient(GenericMQAsyncClient):
    def __init__(self, channel, code='default'):
        self.channel = channel
        self._code = code
        self._exchange = None

    async def exchange(self):
        if not self._exchange:
            mq_server_exchange = getattr(config, 'MQ{}_SERVER_EXCHANGE'.format(_pp(self._code)))
            self._exchange = await self.channel.get_exchange(mq_server_exchange)
        return self._exchange

    async def publish(self, key: str, data: str):
        message = Message(data.encode(), delivery_mode=DeliveryMode.PERSISTENT)
        exchange = await self.exchange()
        await exchange.publish(message, routing_key=key)
        return True

    async def consume(self, key: str, on_message, no_ack=True):
        # TODO This is minimally functional code which should not be used.
        #  It needs a lot more work if actual asyncio consumers are required.
        queue = await self.channel.declare_queue(key)
        await queue.consume(on_message)

    async def create_work_queue(self, name: str, topic: str):
        queue = await self.channel.declare_queue(name, durable=True, auto_delete=False)
        exchange = await self.exchange()
        await queue.bind(exchange, routing_key=topic)


manager = RabbitMQAsyncManager
client_class = RabbitMQAsyncClient

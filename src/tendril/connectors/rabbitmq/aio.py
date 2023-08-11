

from pika import ConnectionParameters
from contextlib import asynccontextmanager
from aio_pika import Message
from aio_pika import connect_robust
from aio_pika import DeliveryMode
from aio_pika import ExchangeType
from aio_pika.pool import Pool

from tendril.core.mq.aio_base import GenericMQAsyncClient
from tendril.core.mq.aio_base import GenericMQAsyncManager
from tendril.config import MQ_SERVER_PARAMETERS
from tendril.config import MQ_SERVER_SSL
from tendril.config import MQ_SERVER_EXCHANGE

from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


if MQ_SERVER_SSL:
    scheme = 'amqps'
else:
    scheme = 'amqp'

p: ConnectionParameters = MQ_SERVER_PARAMETERS
connection_string = f"{scheme}://{p.credentials.username}:{p.credentials.password}" \
                    f"@{p.host}:{p.port}/{p.virtual_host}"


class RabbitMQAsyncManager(GenericMQAsyncManager):
    _instance = None

    @classmethod
    async def get_instance(cls):
        if cls._instance is None:
            cls._instance = RabbitMQAsyncManager()
        return cls._instance

    async def init(self, loop):
        # Use the connect function to create a connection object with the default parameters
        # The connection object will be reused until it is closed explicitly
        logger.info(f"Connecting to RabbitMQ at {MQ_SERVER_PARAMETERS.host}")
        logger.debug(f"Using connection string {connection_string}")
        self.connection = await connect_robust(url=connection_string, loop=loop,
                                               client_properties={'name': log.identifier})

        async def get_channel():
            logger.debug(f"Creating a channel to RabbitMQ at {MQ_SERVER_PARAMETERS.host}")
            channel = await self.connection.channel()
            await channel.set_qos(prefetch_count=1)
            return channel
        self.channel_pool: Pool = Pool(get_channel, max_size=10, loop=loop)

        async with self.channel_pool.acquire() as channel:
            logger.debug(f"Creating the {MQ_SERVER_EXCHANGE} exchange on "
                         f"RabbitMQ at {MQ_SERVER_PARAMETERS.host}")
            _exchange = await channel.declare_exchange(
                MQ_SERVER_EXCHANGE, ExchangeType.TOPIC, durable=True
            )

    async def close(self):
        logger.info(f"Closing the channel pool to RabbitMQ at {MQ_SERVER_PARAMETERS.host}")
        await self.channel_pool.close()
        self.channel_pool = None
        logger.info(f"Disconnecting from RabbitMQ at {MQ_SERVER_PARAMETERS.host}")
        await self.connection.close()
        self.connection = None

    @asynccontextmanager
    async def get_channel(self):
        async with self.channel_pool.acquire() as channel:
            yield channel


class RabbitMQAsyncClient(GenericMQAsyncClient):
    def __init__(self, channel):
        self.channel = channel
        self._exchange = None

    async def exchange(self):
        if not self._exchange:
            self._exchange = await self.channel.get_exchange(MQ_SERVER_EXCHANGE)
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

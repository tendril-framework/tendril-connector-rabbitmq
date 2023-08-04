

from aio_pika import connect
from aio_pika import Message
from aio_pika.pool import Pool

from tendril.core.mq.async_base import GenericMQAsyncClient
from tendril.core.mq.async_base import GenericMQAsyncManager


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
        self.connection = await connect()

        async def get_channel():
            return await self.connection.channel()
        self.channel_pool: Pool = Pool(get_channel, max_size=10, loop=loop)

    async def close(self):
        await self.channel_pool.close()
        self.channel_pool = None
        await self.connection.close()
        self.connection = None

    async def get_channel(self):
        async with self.channel_pool.acquire() as channel:
            yield channel


class RabbitMQAsyncClient(GenericMQAsyncClient):
    def __init__(self, channel):
        self.channel = channel

    async def send_message(self, queue: str, data: str):
        queue = await self.channel.declare_queue(queue)
        message = Message(data.encode())
        await queue.publish(message)
        return True

    async def receive_message(self, queue: str, no_wait=False, no_ack=False):
        queue = await self.channel.declare_queue(queue)
        message = await queue.get(no_wait=no_wait, no_ack=no_ack)
        if message:
            return message.body.decode()
        else:
            return ""


manager = RabbitMQAsyncManager
client_class = RabbitMQAsyncClient

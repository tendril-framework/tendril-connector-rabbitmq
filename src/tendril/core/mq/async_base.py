

class GenericMQAsyncManager:
    async def init(self, loop):
        raise NotImplementedError

    async def close(self):
        raise NotImplementedError

    async def get_channel(self):
        raise NotImplementedError


class GenericMQAsyncClient(object):
    async def send_message(self, queue: str, data: str):
        raise NotImplementedError

    async def receive_message(self, queue: str, no_wait=False, no_ack=False):
        raise NotImplementedError

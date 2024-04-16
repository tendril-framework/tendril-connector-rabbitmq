


class MQServerError(Exception):
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return f'code={self.code}'

class MQServerNotRecognized(MQServerError):
    pass

class MQServerNotEnabled(MQServerError):
    pass


class GenericMQAsyncManager:
    async def init(self, loop):
        raise NotImplementedError

    async def close(self):
        raise NotImplementedError

    async def get_channel(self, code='default'):
        raise NotImplementedError


class GenericMQAsyncClient(object):
    async def publish(self, key: str, data: str):
        raise NotImplementedError

    async def consume(self, key: str, on_message, no_ack=True):
        raise NotImplementedError

    async def create_worker_queue(self, key: str, topic: str):
        raise NotImplementedError



import asyncio
import atexit
from functools import wraps

from .async_base import GenericMQAsyncClient
from tendril.config import APISERVER_ENABLED


if True:
    from tendril.connectors.rabbitmq import aio
    MQManager = aio.manager
    MQClientClass = aio.client_class


def with_mq_client(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        mq = kwargs.pop('mq', None)
        if not mq:
            manager = await MQManager.get_instance()
            async with manager.get_channel() as channel:
                mq = MQClientClass(channel)
        kwargs['mq'] = mq
        return await func(*args, **kwargs)
    return wrapper


@with_mq_client  # Use the decorator to inject a client object
async def example(mq: GenericMQAsyncClient):  # Accept a client object as an argument
    await mq.send_message("test", "Hello world")
    message = await mq.receive_message("test")
    print(message)


async def startup():
    manager = await MQManager.get_instance()
    await manager.init(loop=asyncio.get_running_loop())


async def shutdown():
    manager = await MQManager.get_instance()
    await manager.close()


if APISERVER_ENABLED:
    # Register the startup and shutdown functions as app events
    from tendril.apiserver.core import apiserver
    apiserver.on_event("startup")(startup)
    apiserver.on_event("shutdown")(shutdown)
else:
    # TODO This doesn't actually work.
    #  Find a serious use case and a corresponding solution.
    # asyncio.run(startup())
    # atexit.register(asyncio.run, shutdown())
    pass


# # Define an endpoint to send a message to a queue
# @app.post("/send/{queue}")
# async def send_message(queue: str, data: str, background_tasks: BackgroundTasks):
#     # Run the test function as a background task using the background_tasks object
#     await run_background_task(background_tasks, test)
#     # Return a success message as JSON response
#     return {"message": "Message sent successfully"}
#
# # Define an endpoint to receive a message from a queue
# @app.get("/receive/{queue}")
# async def receive_message(queue: str, background_tasks: BackgroundTasks):
#     # Run the test function as a background task using the background_tasks object
#     await run_background_task(background_tasks, test)
#     # Return a success message as JSON response
#     return {"message": "Message received successfully"}
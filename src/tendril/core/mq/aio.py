

import asyncio
import importlib
from functools import wraps

from .aio_base import GenericMQAsyncClient
from .aio_base import MQServerNotEnabled
from tendril import config
from tendril.config import MQ_SERVER_CODES
from tendril.config import APISERVER_ENABLED
from tendril.utils.versions import get_namespace_package_names
from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


if True:
    from tendril.connectors.rabbitmq import aio
    MQManager = aio.manager
    MQClientClass = aio.client_class


def with_mq_client(code='default'):
    def wrapper(func):
        @wraps(func)
        async def inner_wrapper(*args, **kwargs):
            if 'mq' in kwargs.keys() and kwargs['mq']:
                result = await func(*args, **kwargs)
                return result
            manager = await MQManager.get_instance()
            async with manager.get_channel(code=code) as channel:
                mq = MQClientClass(channel, code=code)
                kwargs['mq'] = mq
                result = await func(*args, **kwargs)
                return result
        return inner_wrapper
    return wrapper


@with_mq_client()  # Use the decorator to inject a client object
async def example(mq: GenericMQAsyncClient):  # Accept a client object as an argument
    await mq.send_message("test", "Hello world")
    message = await mq.receive_message("test")
    print(message)


async def install_topology(prefix='tendril.core.topology'):
    logger.info("Loading MQ Topologies from '{0}.*'".format(prefix))
    for modname in get_namespace_package_names(prefix):
        try:
            globals()[modname] = importlib.import_module(modname)
            if hasattr(globals()[modname], 'create_mq_topology'):
                logger.debug(f"Installing MQ Topology from {modname}")
                await globals()[modname].create_mq_topology()
        except MQServerNotEnabled as e:
            logger.info(f"Skipping topology installation on disabled MQ Server {e.code}")
        except ImportError as e:
            logger.debug(e)


async def startup():
    manager = await MQManager.get_instance()
    await manager.init(loop=asyncio.get_running_loop())
    await install_topology()


async def shutdown():
    manager = await MQManager.get_instance()
    await manager.close()


def _enable():
    if APISERVER_ENABLED:
        # Register the startup and shutdown functions as app events
        from tendril.apiserver.core import apiserver

        apiserver.on_event("startup")(startup)
        apiserver.on_event("shutdown")(shutdown)
    else:
        asyncio.get_event_loop().run_until_complete(startup())
        # atexit.register(asyncio.run, shutdown())


for code in MQ_SERVER_CODES:
    if getattr(config, 'MQ{}_ENABLED'.format(code)):
        _enable()
        break

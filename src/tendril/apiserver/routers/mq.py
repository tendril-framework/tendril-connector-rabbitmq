

from fastapi import APIRouter
from fastapi import Depends
from fastapi import BackgroundTasks

from tendril.authn.users import authn_dependency
from tendril.authn.users import AuthUserModel
from tendril.authn.users import auth_spec

from tendril.core.mq.aio import with_mq_client
from tendril.core.mq.async_base import GenericMQAsyncClient


mq_router = APIRouter(prefix='/mq',
                      tags=["Low Level Message Queue Access"],
                      dependencies=[Depends(authn_dependency)])


@with_mq_client
async def simple_send(queue, msg, mq: GenericMQAsyncClient=None):
    print(f"Sending {msg} to {queue}")
    await mq.send_message(queue, msg)


@with_mq_client
async def simple_receive(queue, no_wait=True, no_ack=True,
                         mq: GenericMQAsyncClient=None):
    msg = await mq.receive_message(queue, no_wait=no_wait, no_ack=no_ack)
    print(f"Got {msg} from {queue}")


@mq_router.post("/send/{queue}")
async def send_message(queue: str, msg: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(simple_send, queue, msg)
    return {"message": "Message sent successfully"}


# Define an endpoint to receive a message from a queue
@mq_router.get("/receive/{queue}")
async def receive_message(queue: str, background_tasks: BackgroundTasks,
                          no_ack=True, no_wait=True):
    background_tasks.add_task(simple_receive, queue,
                              no_wait=no_wait, no_ack=no_ack)
    return {"message": "Message receive triggered"}

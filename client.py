from __future__ import with_statement
from queues import task_exchange
from kombu import Queue
import parsers

from kombu.common import maybe_declare
from kombu.common import drain_consumer
from kombu import Connection


from uuid import uuid4

import json



def rpc_call(uri="amqp://guest:guest@localhost:5672//", peer='TO_SERV_1',
             fname="parse_dirs()", *args, **kwargs):

    payload = {'fname': fname}
    if args:
        payload['args'] = args
    if kwargs:
        payload['kwargs'] = kwargs

    call_id = str(uuid4())
    answer_queue = Queue(call_id,
                         task_exchange,
                         routing_key=call_id,
                         auto_delete=True,
                         durable=False)

    print ("[X] BACK QUEUE CREATED!")
    with Connection(uri) as connection:
        with connection.Producer() as producer:
            maybe_declare(answer_queue, producer.channel)
            producer.publish(payload,
                             serializer='json',
                             exchange=task_exchange,
                             routing_key=peer,
                             correlation_id='corr1',
                             reply_to=call_id)
            print ("[X] Message published, waiting for answer.".upper())
        with connection.Consumer(answer_queue) as consumer:
            for body, msg in drain_consumer(consumer, timeout=10):
                print ("[X] Answer catched. \nBody:".upper())
                msg.ack()
                print body
                print("---------------------------------")


if __name__ == "__main__":

    URI = parsers.host
    PEER = parsers.peer
    COMMAND = parsers.command
    FILE = parsers.currentfile
    DIR = parsers.currentpath

    rpc_call(uri=URI,peer=PEER,folder=DIR)

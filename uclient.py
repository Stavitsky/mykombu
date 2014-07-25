from __future__ import with_statement
from queues import task_exchange
from kombu import Queue
import parsers.client as cpars

from kombu.common import maybe_declare
from kombu.common import drain_consumer
from kombu import Connection

from _librabbitmq import ConnectionError

from uuid import uuid4

import json

URI = cpars.host
PEER = cpars.peer
COMMAND = cpars.command
FILE = cpars.currentfile
DIR = cpars.currentpath


def rpc_call(uri="amqp://guest:guest@localhost:5672//", peer='TO_SERV_1',
             fname="parse_dirs", *args, **kwargs):

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
            try:
                maybe_declare(answer_queue, producer.channel)
            except ConnectionError as cerr:
                print("[ERROR] %s . HOST:'%s'" % (cerr.message.upper(), URI))
                print("---------------------------------")
                return
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
                if COMMAND == 'write':
                    with open(FILE, 'w') as outfile:
                        json.dump(body, outfile, indent=3)
                    print ("[X] DATA WROTE TO FILE (%s)." % (FILE))
                if COMMAND == 'check':
                    print ("[X] HERE MUST BE CHECKING RESULT")

                print("---------------------------------")


def main():
    rpc_call(uri=URI, peer=PEER, folder=DIR)


if __name__ == "__main__":
    main()

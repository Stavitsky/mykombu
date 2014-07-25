from __future__ import with_statement
from queues import task_exchange
from kombu import Queue

from kombu.common import maybe_declare
from kombu.common import drain_consumer
from kombu import Connection


from uuid import uuid4

import json


def rpc_call(uri, peer, fname, call_id):
    print ("[CATCHED METHOD] rpc_call()")
    payload = {'fname': fname}

    answer_queue = Queue(call_id,
                         task_exchange,
                         routing_key=call_id,
                         auto_delete=True,
                         durable=False)
    print ("Created answer_queue: "+str(answer_queue))

    with Connection(uri) as connection:
        with connection.Producer() as producer:
            maybe_declare(answer_queue, producer.channel)
            producer.publish(payload,
                             serializer='json',
                             exchange=task_exchange,
                             routing_key='TO_SERV_1',
                             correlation_id='corr1',
                             reply_to=call_id)
            print("Message published, wait reply to: "+call_id)
        with connection.Consumer(answer_queue) as consumer:
            for body, msg in drain_consumer(consumer, timeout=600):
                print("Get message back!")
                msg.ack()
                print body, msg


if __name__ == "__main__":
#    from kombu import BrokerConnection

    call_id = str(uuid4())

    rpc_call("amqp://guest:guest@localhost:5672//", 'TO_SERV_1',
             "my_function()", call_id)

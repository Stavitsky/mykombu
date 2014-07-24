from __future__ import with_statement
from queues import task_exchange
from queues import queues_dict
from queues import queue_msg_back
from queues import add_queue

from kombu.mixins import ConsumerMixin
from kombu.common import maybe_declare
from kombu.pools import producers

from uuid import uuid4

import json


class C(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection
        return

    def get_consumers(self, Consumer, channel):
        print ("[!!!] get_consumers() catched!")
        print ("[!!!] call_id here: "+call_id)
        return [Consumer(queues_dict[call_id],
                         accept=['json'],
                         callbacks=[self.on_message])]

    def on_message(self, body, message):
        print ("RECEIVED MSG FROM SERVER - body: %r" % (body,))
        message.ack()

        #data = body.get('content')
        #print (data)
        #with open("outfile.json", "w") as outfile:
        #    json.dump(data, outfile, indent=4)
        #    print("Message body wrote to file")

        return

if __name__ == "__main__":
    from kombu import BrokerConnection

    call_id = str(uuid4())
    add_queue(call_id)

    connection = BrokerConnection("amqp://guest:guest@localhost:5672//")

    with producers[connection].acquire(block=True) as producer:
        maybe_declare(task_exchange, producer.channel)

        payload = {"type": "handshake",
                   "content": "hello serv#1"}

        producer.publish(payload,
                         exchange="msgs",
                         serializer="json",
                         routing_key='TO_SERV_1',
                         correlation_id=call_id,
                         reply_to=call_id)
        print("[!!!] reply_to: " + call_id)

        payload = {"type": "handshake",
                   "content": "hello serv#2"}

        producer.publish(payload,
                         exchange='msgs',
                         serializer="json",
                         routing_key='TO_SERV_2')

        with BrokerConnection("amqp://guest:guest@localhost:5672//") as connection:
            try:
                C(connection).run()
            except KeyboardInterrupt:
                print("bye bye")

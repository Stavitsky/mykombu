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
    def __init__(self, connection, call_id):
        print ("[CATCHED METHOD]__init__()")
        self.connection = connection
        self.id = str(call_id)
        print("[!!!] self.id = "+self.id)
        return

    def get_consumers(self, Consumer, channel, call_id=None):
        print ("[CATCHED METHOD] get_consumers()")
        if call_id is None:
            call_id = self.id

        add_queue(call_id, connection)
        self.send_message()

        return [Consumer(queues_dict[call_id],
                         accept=['json'],
                         callbacks=[self.on_message],
                         auto_declare=True)]

    def send_message(self, connection=None, call_id=None):
        print ("[CATCHED METHOD] send_message()")
        if call_id is None:
            call_id = self.id

        if connection is None:
            connection = self.connection

        with producers[connection].acquire(block=True) as producer:
            maybe_declare(task_exchange, producer.channel)

            payload = {"type": "handshake",
                       "content": "hello serv#1"}

            producer.publish(payload,
                             exchange="msgs",
                             serializer="json",
                             routing_key='TO_SERV_1',
                             correlation_id="cor1",
                             reply_to=call_id)
            print("[!!!] reply_to: " + call_id)

            payload = {"type": "handshake",
                       "content": "hello serv#2"}

            producer.publish(payload,
                             exchange='msgs',
                             serializer="json",
                             routing_key='TO_SERV_2')

    def on_message(self, body, message):
        print ("[CATCHED METHOD] on_message()")
        print ("RECEIVED MSG FROM SERVER - body: %r" % (body,))
        message.ack()

        #data = body.get('content')
        #print (data)
        #with open("outfile.json", "w") as outfile:
        #    json.dump(data, outfile, indent=4)
        #    print("Message body wrote to file")

        exit(0)
        return



if __name__ == "__main__":
    from kombu import BrokerConnection

    call_id = str(uuid4())

    with BrokerConnection("amqp://guest:guest@localhost:5672//") as connection:
        try:
            C(connection, call_id).run()

        except KeyboardInterrupt:
            print("bye bye")

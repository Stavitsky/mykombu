from __future__ import with_statement
from queues import task_exchange
from queues import queues_mass
from queues import queue_msg_back
from queues import add_queue

from kombu.mixins import ConsumerMixin
from kombu.common import maybe_declare
from kombu.pools import producers

import json


class C(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection
        return

    def get_consumers(self, Consumer, channel):

        #n = add_queue('TO_CLIENT')
        #print ("!!!_get_consumers()_CREATE QUENQUE N"+str(n))

        return [Consumer(queue_msg_back,
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

    connection = BrokerConnection("amqp://guest:guest@localhost:5672//")

    with producers[connection].acquire(block=True) as producer:
        maybe_declare(task_exchange, producer.channel)


        payload = {"type": "handshake", "content": "hello serv#1"}
        producer.publish(payload,
                         exchange='msgs',
                         serializer="json",
                         routing_key='TO_SERV_1',
                         correlation_id='back',
                         reply_to='TO_CLIENT')

        payload = {"type": "handshake", "content": "hello serv#2"}
        producer.publish(payload,
                         exchange='msgs',
                         serializer="json",
                         routing_key='TO_SERV_2')

        with BrokerConnection("amqp://guest:guest@localhost:5672//") as connection:
            try:
                C(connection).run()
            except KeyboardInterrupt:
                print("bye bye")
